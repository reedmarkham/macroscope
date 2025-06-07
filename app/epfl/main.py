import os
import sys
import json
import argparse
import time
import gzip
import threading
import logging
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, List

import requests
import tifffile
import numpy as np
from tqdm import tqdm

# Add lib directory to path for config_manager import
sys.path.append('/app/lib')
from config_manager import get_config_manager
from metadata_manager import MetadataManager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


def check_server_capabilities(url: str) -> Tuple[int, bool, bool]:
    """Check server capabilities: file size, range support, compression."""
    logger.info("Checking server capabilities for: %s", url)
    try:
        head_response = requests.head(url, timeout=30)
        head_response.raise_for_status()
        
        # Get file size
        file_size = 0
        if 'content-length' in head_response.headers:
            file_size = int(head_response.headers['content-length'])
            logger.info("File size: %s bytes (%.2f GB)", f"{file_size:,}", file_size / (1024**3))
        
        # Check range support
        accepts_ranges = head_response.headers.get('accept-ranges', '').lower() == 'bytes'
        logger.info("Range requests: %s", 'Supported' if accepts_ranges else 'Not supported')
        
        # Check compression
        content_encoding = head_response.headers.get('content-encoding', '').lower()
        is_compressed = content_encoding in ['gzip', 'deflate', 'br']
        if is_compressed:
            logger.info("Pre-compressed: %s", content_encoding)
        else:
            logger.info("Pre-compressed: No")
        
        return file_size, accepts_ranges, is_compressed
        
    except Exception as e:
        logger.error("Failed to check server capabilities: %s", e)
        return 0, False, False


def download_chunk(url: str, start: int, end: int, chunk_id: int, temp_dir: str) -> Tuple[int, str, int]:
    """Download a specific byte range chunk."""
    chunk_path = os.path.join(temp_dir, f"chunk_{chunk_id}.tmp")
    
    headers = {'Range': f'bytes={start}-{end}'}
    
    try:
        response = requests.get(url, headers=headers, stream=True, timeout=300)
        response.raise_for_status()
        
        downloaded = 0
        with open(chunk_path, 'wb') as f:
            for data in response.iter_content(chunk_size=64*1024):  # 64KB per read
                if data:
                    f.write(data)
                    downloaded += len(data)
        
        return chunk_id, chunk_path, downloaded
        
    except Exception as e:
        logger.error("Chunk %d failed: %s", chunk_id, e)
        return chunk_id, "", 0


def check_existing_file(output_path: str, expected_size: int) -> bool:
    """Check if file already exists and is complete."""
    if os.path.exists(output_path):
        actual_size = os.path.getsize(output_path)
        if actual_size == expected_size:
            logger.info("File already exists and complete: %s (%s bytes)", output_path, f"{actual_size:,}")
            return True
        elif actual_size > 0:
            logger.info("Partial file exists: %s/%s bytes (%.1f%%)", f"{actual_size:,}", f"{expected_size:,}", actual_size/expected_size*100)
            return False
    return False


def download_tif_parallel(url: str, output_path: str, max_workers: int = 4, chunk_size_mb: int = 10) -> None:
    """Download TIFF with parallel chunks, resume capability, and compression detection."""
    # Check server capabilities
    total_size, supports_ranges, is_compressed = check_server_capabilities(url)
    
    if total_size == 0:
        logger.warning("Cannot determine file size, falling back to single-threaded download")
        return download_tif_fallback(url, output_path)
    
    # Check if file already exists and is complete
    if check_existing_file(output_path, total_size):
        return
    
    # If server doesn't support ranges or file is pre-compressed, use single-threaded
    if not supports_ranges or is_compressed:
        reason = "pre-compressed" if is_compressed else "no range support"
        logger.info("Using single-threaded download (%s)", reason)
        return download_tif_fallback(url, output_path)
    
    logger.info("Starting parallel download with %d workers", max_workers)
    logger.info("File size: %s bytes, chunk size: %dMB", f"{total_size:,}", chunk_size_mb)
    
    # Calculate chunks
    chunk_size_bytes = chunk_size_mb * 1024 * 1024
    num_chunks = (total_size + chunk_size_bytes - 1) // chunk_size_bytes
    
    # Limit workers to number of chunks
    actual_workers = min(max_workers, num_chunks)
    print(f"Using {actual_workers} workers for {num_chunks} chunks")
    
    # Create temp directory for chunks
    temp_dir = output_path + ".tmp_chunks"
    os.makedirs(temp_dir, exist_ok=True)
    
    try:
        start_time = time.time()
        downloaded_bytes = 0
        lock = threading.Lock()
        
        # Progress bar for overall download
        progress_bar = tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc="Parallel Download",
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )
        
        def update_progress(bytes_downloaded):
            nonlocal downloaded_bytes
            with lock:
                downloaded_bytes += bytes_downloaded
                progress_bar.update(bytes_downloaded)
        
        # Download chunks in parallel
        chunk_futures = []
        with ThreadPoolExecutor(max_workers=actual_workers) as executor:
            for i in range(num_chunks):
                start_byte = i * chunk_size_bytes
                end_byte = min(start_byte + chunk_size_bytes - 1, total_size - 1)
                
                future = executor.submit(download_chunk, url, start_byte, end_byte, i, temp_dir)
                chunk_futures.append(future)
            
            # Collect results and update progress
            chunk_results = {}
            for future in as_completed(chunk_futures):
                chunk_id, chunk_path, chunk_bytes = future.result()
                if chunk_path:  # Success
                    chunk_results[chunk_id] = chunk_path
                    update_progress(chunk_bytes)
                else:  # Failed
                    progress_bar.close()
                    raise Exception(f"Chunk {chunk_id} download failed")
        
        progress_bar.close()
        
        # Combine chunks
        print(f"Combining {len(chunk_results)} chunks...")
        with open(output_path, 'wb') as output_file:
            for chunk_id in sorted(chunk_results.keys()):
                chunk_path = chunk_results[chunk_id]
                with open(chunk_path, 'rb') as chunk_file:
                    output_file.write(chunk_file.read())
                os.remove(chunk_path)  # Clean up
        
        # Clean up temp directory
        os.rmdir(temp_dir)
        
        # Final verification
        actual_size = os.path.getsize(output_path)
        total_time = time.time() - start_time
        avg_speed = actual_size / total_time if total_time > 0 else 0
        
        print(f"\nParallel download completed!")
        print(f"Final stats: {actual_size/(1024*1024):.1f} MB in {total_time:.1f}s (avg: {avg_speed/(1024*1024):.1f} MB/s)")
        print(f"Saved to: {output_path}")
        
        if actual_size == total_size:
            print(f"File size verification: PASSED ({actual_size:,} bytes)")
        else:
            print(f"File size mismatch: downloaded {actual_size:,}, expected {total_size:,}")
            
    except Exception as e:
        print(f"Parallel download failed: {e}")
        print("Falling back to single-threaded download...")
        
        # Clean up on failure
        if os.path.exists(temp_dir):
            for file in os.listdir(temp_dir):
                os.remove(os.path.join(temp_dir, file))
            os.rmdir(temp_dir)
        
        # Fallback to single-threaded
        download_tif_fallback(url, output_path)


def download_tif_fallback(url: str, output_path: str) -> None:
    """Fallback single-threaded download with resume capability."""
    # Check for partial download
    resume_pos = 0
    if os.path.exists(output_path):
        resume_pos = os.path.getsize(output_path)
        if resume_pos > 0:
            print(f"Resuming download from byte {resume_pos:,}")
    
    headers = {}
    if resume_pos > 0:
        headers['Range'] = f'bytes={resume_pos}-'
    
    print(f"Starting download from: {url}")
    response = requests.get(url, headers=headers, stream=True, timeout=1800)
    response.raise_for_status()
    
    # Get total size (accounting for partial download)
    total_size = resume_pos
    if 'content-length' in response.headers:
        total_size += int(response.headers['content-length'])
    
    # Enhanced progress tracking
    chunk_size = 8 * 1024 * 1024  # 8MB chunks
    downloaded_bytes = resume_pos
    start_time = time.time()
    last_update_time = start_time
    last_downloaded_bytes = resume_pos
    
    progress_bar = tqdm(
        total=total_size,
        initial=resume_pos,
        unit='B',
        unit_scale=True,
        unit_divisor=1024,
        desc="Downloading",
        bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
    )
    
    try:
        mode = 'ab' if resume_pos > 0 else 'wb'
        with open(output_path, mode) as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    chunk_len = len(chunk)
                    downloaded_bytes += chunk_len
                    progress_bar.update(chunk_len)
                    
                    # Update progress info every 10 seconds
                    current_time = time.time()
                    if current_time - last_update_time >= 10.0:
                        speed_bytes_sec = (downloaded_bytes - last_downloaded_bytes) / (current_time - last_update_time)
                        
                        if total_size > 0:
                            percentage = (downloaded_bytes / total_size) * 100
                            remaining_bytes = total_size - downloaded_bytes
                            eta_seconds = remaining_bytes / speed_bytes_sec if speed_bytes_sec > 0 else 0
                            
                            if eta_seconds > 3600:
                                eta_str = f"{eta_seconds/3600:.1f}h"
                            elif eta_seconds > 60:
                                eta_str = f"{eta_seconds/60:.1f}m"
                            else:
                                eta_str = f"{eta_seconds:.0f}s"
                            
                            print(f"\nProgress: {percentage:.1f}% | Speed: {speed_bytes_sec/(1024*1024):.1f} MB/s | ETA: {eta_str}")
                        
                        last_update_time = current_time
                        last_downloaded_bytes = downloaded_bytes
        
        progress_bar.close()
        
        # Final statistics
        total_time = time.time() - start_time
        net_downloaded = downloaded_bytes - resume_pos
        avg_speed = net_downloaded / total_time if total_time > 0 else 0
        
        print(f"\nDownload completed successfully!")
        if resume_pos > 0:
            print(f"Resumed from: {resume_pos/(1024*1024):.1f} MB")
        print(f"Final stats: {downloaded_bytes/(1024*1024):.1f} MB total, {net_downloaded/(1024*1024):.1f} MB new in {total_time:.1f}s")
        print(f"Average speed: {avg_speed/(1024*1024):.1f} MB/s")
        print(f"Saved to: {output_path}")
        
    except Exception as e:
        progress_bar.close()
        print(f"Download failed: {e}")
        raise


def download_tif(url: str, output_path: str, max_workers: int = 4) -> None:
    """Main download function with automatic optimization selection."""
    try:
        download_tif_parallel(url, output_path, max_workers)
    except Exception as e:
        print(f"Parallel download failed: {e}")
        print("Falling back to single-threaded download...")
        download_tif_fallback(url, output_path)


def load_volume(tif_path: str) -> np.ndarray:
    with tifffile.TiffFile(tif_path) as tif:
        data = tif.asarray()
    print(f"Loaded volume shape: {data.shape}, dtype: {data.dtype}")
    return data


def save_volume(volume: np.ndarray, output_path: str) -> None:
    np.save(output_path, volume)
    print(f"Saved volume as .npy to {output_path}")


def write_metadata(volume: np.ndarray, tif_path: str, npy_path: str, timestamp: str, source_id: str, download_url: str, output_dir: str, voxel_size_nm: list) -> None:
    metadata_filename = f"metadata_{timestamp}.json"
    metadata_path = os.path.join(output_dir, metadata_filename)
    
    # Initialize MetadataManager
    metadata_manager = MetadataManager()
    
    # Create standardized metadata record
    record = metadata_manager.create_metadata_record(
        source="epfl",
        source_id=source_id,
        description="5x5x5µm section from CA1 hippocampus region"
    )
    
    # Add technical metadata
    metadata_manager.add_technical_metadata(
        record,
        volume_shape=list(volume.shape),
        voxel_size_nm=voxel_size_nm,
        data_type=str(volume.dtype),
        file_size_bytes=volume.nbytes
    )
    
    # Add file paths
    metadata_manager.add_file_paths(
        record,
        volume_path=npy_path,
        raw_path=tif_path,
        metadata_path=metadata_path
    )
    
    # Add provenance information
    if "provenance" not in record["metadata"]:
        record["metadata"]["provenance"] = {}
    record["metadata"]["provenance"]["download_url"] = download_url
    
    # Update status to complete
    metadata_manager.update_status(record, "complete")
    
    # Save with validation
    metadata_manager.save_metadata(record, metadata_path, validate=True)
    print(f"Saved metadata to {metadata_path}")


def ingest_epfl_tif(config) -> None:
    # Get configuration values
    source_id = config.get('sources.epfl.defaults.source_id', 'EPFL-CA1-HIPPOCAMPUS')
    download_url = config.get('sources.epfl.defaults.download_url', 'https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/volumedata.tif')
    output_dir = os.environ.get('EM_DATA_DIR', config.get('sources.epfl.output_dir', './data/epfl'))
    voxel_size_nm = config.get('sources.epfl.defaults.voxel_size_nm', [5.0, 5.0, 5.0])
    max_workers = config.get('sources.epfl.defaults.max_workers', 4)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tif_filename = f"volumedata_{timestamp}.tif"
    npy_filename = f"volumedata_{timestamp}.npy"
    tif_path = os.path.join(output_dir, tif_filename)
    npy_path = os.path.join(output_dir, npy_filename)

    download_tif(download_url, tif_path, max_workers)
    volume = load_volume(tif_path)
    save_volume(volume, npy_path)
    write_metadata(volume, tif_path, npy_path, timestamp, source_id, download_url, output_dir, voxel_size_nm)


def parse_args():
    parser = argparse.ArgumentParser(description='EPFL CVLab data ingestion')
    parser.add_argument('--config', type=str, default=None,
                        help='Path to configuration file')
    parser.add_argument('--source-id', type=str, default=None,
                        help='Source ID for the dataset')
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Initialize config manager
    config_manager = get_config_manager(args.config)
    
    # Override source_id if provided via command line
    if args.source_id:
        config_manager.set('sources.epfl.defaults.source_id', args.source_id)
    
    ingest_epfl_tif(config_manager)


if __name__ == "__main__":
    main()
