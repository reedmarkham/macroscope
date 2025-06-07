import os
import sys
import json
import argparse
import time
from datetime import datetime

import requests
import tifffile
import numpy as np
from tqdm import tqdm

# Add lib directory to path for config_manager import
sys.path.append('/app/lib')
from config_manager import get_config_manager


def get_file_size(url: str) -> int:
    """Get file size using HEAD request before downloading."""
    print(f"🔍 Getting file size for: {url}")
    try:
        head_response = requests.head(url, timeout=30)
        head_response.raise_for_status()
        
        file_size = head_response.headers.get('content-length')
        if file_size:
            file_size = int(file_size)
            print(f"📊 File size: {file_size:,} bytes ({file_size / (1024**3):.2f} GB)")
            return file_size
        else:
            print("⚠️  Could not determine file size from headers")
            return 0
    except Exception as e:
        print(f"⚠️  Failed to get file size: {e}")
        return 0


def download_tif(url: str, output_path: str) -> None:
    """Download TIFF with enhanced progress tracking."""
    # Get total file size first
    total_size = get_file_size(url)
    
    print(f"🚀 Starting download from: {url}")
    response = requests.get(url, stream=True, timeout=1800)  # 30 min timeout
    response.raise_for_status()
    
    # Enhanced progress tracking
    chunk_size = 8 * 1024 * 1024  # 8MB chunks
    downloaded_bytes = 0
    start_time = time.time()
    last_update_time = start_time
    last_downloaded_bytes = 0
    
    # Calculate total chunks for more accurate progress
    if total_size > 0:
        total_chunks = (total_size + chunk_size - 1) // chunk_size
        progress_bar = tqdm(
            total=total_size,
            unit='B',
            unit_scale=True,
            unit_divisor=1024,
            desc="📥 Downloading",
            bar_format="{desc}: {percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]"
        )
    else:
        progress_bar = tqdm(
            unit='B',
            unit_scale=True, 
            unit_divisor=1024,
            desc="📥 Downloading",
            bar_format="{desc}: {n_fmt} [{elapsed}, {rate_fmt}]"
        )
    
    try:
        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)
                    chunk_len = len(chunk)
                    downloaded_bytes += chunk_len
                    progress_bar.update(chunk_len)
                    
                    # Update progress info every 10 seconds
                    current_time = time.time()
                    if current_time - last_update_time >= 10.0:
                        elapsed = current_time - start_time
                        speed_bytes_sec = (downloaded_bytes - last_downloaded_bytes) / (current_time - last_update_time)
                        
                        if total_size > 0:
                            percentage = (downloaded_bytes / total_size) * 100
                            remaining_bytes = total_size - downloaded_bytes
                            eta_seconds = remaining_bytes / speed_bytes_sec if speed_bytes_sec > 0 else 0
                            
                            # Format ETA nicely
                            if eta_seconds > 3600:
                                eta_str = f"{eta_seconds/3600:.1f}h"
                            elif eta_seconds > 60:
                                eta_str = f"{eta_seconds/60:.1f}m"
                            else:
                                eta_str = f"{eta_seconds:.0f}s"
                            
                            print(f"\n📊 Progress: {percentage:.1f}% | Speed: {speed_bytes_sec/(1024*1024):.1f} MB/s | ETA: {eta_str}")
                        else:
                            print(f"\n📊 Downloaded: {downloaded_bytes/(1024*1024):.1f} MB | Speed: {speed_bytes_sec/(1024*1024):.1f} MB/s")
                        
                        last_update_time = current_time
                        last_downloaded_bytes = downloaded_bytes
        
        progress_bar.close()
        
        # Final statistics
        total_time = time.time() - start_time
        avg_speed = downloaded_bytes / total_time if total_time > 0 else 0
        
        print(f"\n✅ Download completed successfully!")
        print(f"📊 Final stats: {downloaded_bytes/(1024*1024):.1f} MB in {total_time:.1f}s (avg: {avg_speed/(1024*1024):.1f} MB/s)")
        print(f"💾 Saved to: {output_path}")
        
        # Verify file size if we had the total
        if total_size > 0:
            if downloaded_bytes == total_size:
                print(f"✅ File size verification: PASSED ({downloaded_bytes:,} bytes)")
            else:
                print(f"⚠️  File size mismatch: downloaded {downloaded_bytes:,}, expected {total_size:,}")
                
    except Exception as e:
        progress_bar.close()
        print(f"❌ Download failed: {e}")
        raise


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
    metadata = {
        "source": "epfl",
        "source_id": source_id,
        "description": "5x5x5µm section from CA1 hippocampus region",
        "volume_shape": list(volume.shape),
        "voxel_size_nm": voxel_size_nm,
        "download_url": download_url,
        "local_paths": {
            "volume": npy_path,
            "raw": tif_path,
            "metadata": metadata_path
        }
    }
    
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to {metadata_path}")


def ingest_epfl_tif(config) -> None:
    # Get configuration values
    source_id = config.get('sources.epfl.defaults.source_id', 'EPFL-CA1-HIPPOCAMPUS')
    download_url = config.get('sources.epfl.defaults.download_url', 'https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/volumedata.tif')
    output_dir = config.get('sources.epfl.output_dir', './data/epfl')
    voxel_size_nm = config.get('sources.epfl.defaults.voxel_size_nm', [5.0, 5.0, 5.0])
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tif_filename = f"volumedata_{timestamp}.tif"
    npy_filename = f"volumedata_{timestamp}.npy"
    tif_path = os.path.join(output_dir, tif_filename)
    npy_path = os.path.join(output_dir, npy_filename)

    download_tif(download_url, tif_path)
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
