import os
import sys
import json
import argparse
import hashlib
import time
import socket
from datetime import datetime
from ftplib import FTP, error_perm, error_temp
from typing import Any, Dict, List, Optional, Tuple, Union

import numpy as np
import tifffile
import requests

# Add lib directory to path for config_manager import
sys.path.append('/app/lib')
from config_manager import get_config_manager


def get_image_ids_from_dataset(dataset_id: Union[str, int], api_base_url: str) -> List[Tuple[int, str]]:
    url = f"{api_base_url}/m/images/?dataset={dataset_id}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]
    return [(img["@id"], img.get("Name", f"Image {img['@id']}")) for img in data]


def fetch_image_name_and_dataset_dir(image_id: Union[str, int], api_base_url: str, dataset_id: str, path_mappings: Dict[str, str]) -> Tuple[str, str]:
    url = f"{api_base_url}/m/images/{image_id}/"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]

    image_name: Optional[str] = data.get("Name")
    if not image_name:
        raise ValueError("No image name found in metadata.")

    dataset_dir: Optional[str] = path_mappings.get(dataset_id)
    if not dataset_dir:
        raise ValueError(f"No hardcoded dataset path found for {dataset_id}.")

    return image_name, dataset_dir


def construct_ftp_path_from_name(dataset_dir: str, image_name: str, ftp_root_path: str) -> str:
    return f"{ftp_root_path}/{dataset_dir}/{image_name}"


def download_via_ftp(image_id: Union[str, int], ftp_path: str, timestamp: str, ftp_host: str, output_dir: str, max_retries: int = 3) -> Optional[str]:
    """Download file via FTP with robust error handling and retry logic."""
    local_path = os.path.join(output_dir, f"{image_id}_{timestamp}.tif")
    
    for attempt in range(max_retries):
        ftp = None
        try:
            print(f"🔗 Attempting FTP connection to {ftp_host} (attempt {attempt + 1}/{max_retries})")
            
            # Create FTP connection with timeout
            ftp = FTP()
            ftp.set_debuglevel(0)  # Disable debug output
            
            # Set socket timeout for connection
            socket.setdefaulttimeout(30)  # 30 second timeout
            
            # Connect with explicit timeout handling
            try:
                ftp.connect(ftp_host, timeout=30)
                print(f"✅ Connected to {ftp_host}")
            except (socket.timeout, socket.gaierror, ConnectionRefusedError, EOFError) as e:
                print(f"⚠️ Connection failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff
                    print(f"⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Failed to connect after {max_retries} attempts")
                    return None
            
            # Login (anonymous)
            try:
                ftp.login()
                print(f"✅ Logged in successfully")
            except (error_perm, error_temp, EOFError) as e:
                print(f"⚠️ Login failed: {e}")
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Login failed after {max_retries} attempts")
                    return None
            
            # Download file
            try:
                with open(local_path, "wb") as f:
                    print(f"📥 FTP downloading {ftp_path}")
                    
                    # Set binary mode and download with progress
                    ftp.voidcmd('TYPE I')  # Set binary mode
                    
                    # Get file size for progress tracking
                    try:
                        file_size = ftp.size(ftp_path)
                        print(f"📊 File size: {file_size / (1024*1024):.1f} MB" if file_size else "📊 File size: unknown")
                    except:
                        file_size = None
                    
                    # Download with retry on temporary failures
                    def write_with_progress(data):
                        f.write(data)
                        if hasattr(write_with_progress, 'downloaded'):
                            write_with_progress.downloaded += len(data)
                        else:
                            write_with_progress.downloaded = len(data)
                    
                    write_with_progress.downloaded = 0
                    ftp.retrbinary(f"RETR {ftp_path}", write_with_progress)
                    
                    downloaded_mb = write_with_progress.downloaded / (1024*1024)
                    print(f"✅ Download complete: {downloaded_mb:.1f} MB")
                    
            except (error_perm, error_temp, EOFError, socket.timeout) as e:
                print(f"⚠️ Download failed: {e}")
                if os.path.exists(local_path):
                    os.remove(local_path)  # Clean up partial download
                
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt
                    print(f"⏳ Waiting {wait_time}s before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    print(f"❌ Download failed after {max_retries} attempts")
                    return None
            
            # If we get here, download was successful
            return local_path
            
        except Exception as e:
            print(f"❌ Unexpected error during FTP operation: {e}")
            if os.path.exists(local_path):
                os.remove(local_path)  # Clean up partial download
            
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"⏳ Waiting {wait_time}s before retry...")
                time.sleep(wait_time)
                continue
            else:
                print(f"❌ FTP operation failed after {max_retries} attempts")
                return None
                
        finally:
            # Always try to close the connection
            if ftp:
                try:
                    ftp.quit()
                except:
                    try:
                        ftp.close()
                    except:
                        pass
            # Reset socket timeout
            socket.setdefaulttimeout(None)
    
    return None


def download_via_http_fallback(image_id: Union[str, int], api_base_url: str, timestamp: str, output_dir: str) -> Optional[str]:
    """Fallback HTTP download method for when FTP fails."""
    try:
        print(f"🌐 Attempting HTTP fallback download for image {image_id}")
        
        # Construct download URL (this is IDR-specific)
        download_url = f"{api_base_url}/webclient/render_image_download/{image_id}/"
        
        local_path = os.path.join(output_dir, f"{image_id}_{timestamp}_http.tif")
        
        # Download with requests and streaming
        response = requests.get(download_url, stream=True, timeout=300)  # 5 minute timeout
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        print(f"📊 HTTP download size: {total_size / (1024*1024):.1f} MB" if total_size else "📊 HTTP download size: unknown")
        
        downloaded = 0
        with open(local_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
        
        downloaded_mb = downloaded / (1024*1024)
        print(f"✅ HTTP download complete: {downloaded_mb:.1f} MB")
        return local_path
        
    except Exception as e:
        print(f"❌ HTTP fallback download failed: {e}")
        if os.path.exists(local_path):
            os.remove(local_path)
        return None


def load_image(image_path: str) -> np.ndarray:
    with tifffile.TiffFile(image_path) as tif:
        data = tif.asarray()
    print(f"✅ Loaded image with shape: {data.shape}")
    return data


def write_metadata_stub(
    image_id: Union[str, int],
    image_name: str,
    npy_path: str,
    metadata_path: str,
    ftp_url: str
) -> Dict[str, Any]:
    stub: Dict[str, Any] = {
        "source": "idr",
        "source_id": f"IDR-{image_id}",
        "description": image_name,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "download_url": ftp_url,
        "local_paths": {
            "volume": npy_path,
            "metadata": metadata_path
        },
        "status": "saving-data"
    }
    tmp = metadata_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(stub, f, indent=2)
    os.replace(tmp, metadata_path)
    return stub


def enrich_metadata(
    metadata_path: str,
    stub: Dict[str, Any],
    data: np.ndarray
) -> None:
    stub.update({
        "volume_shape": list(data.shape),
        "file_size_bytes": data.nbytes,
        "sha256": hashlib.sha256(data.tobytes()).hexdigest(),
        "status": "complete"
    })
    tmp = metadata_path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(stub, f, indent=2)
    os.replace(tmp, metadata_path)


def ingest_image_via_ftp(config) -> None:
    # Get configuration values
    dataset_id = config.get('sources.idr.defaults.dataset_id', 'idr0086')
    image_ids = config.get('sources.idr.defaults.image_ids', [9846137])
    api_base_url = config.get('sources.idr.base_urls.api', 'https://idr.openmicroscopy.org/api/v0')
    ftp_host = config.get('sources.idr.defaults.ftp_host', 'ftp.ebi.ac.uk')
    ftp_root_path = config.get('sources.idr.defaults.ftp_root_path', '/pub/databases/IDR')
    output_dir = config.get('sources.idr.output_dir', './data/idr')
    max_retries = config.get('sources.idr.processing.max_retries', 3)
    
    print(f"🚀 Starting IDR ingestion with robust download (max retries: {max_retries})")
    print(f"📊 Processing {len(image_ids)} images from dataset {dataset_id}")
    print(f"🔗 FTP host: {ftp_host}")
    print(f"🌐 API: {api_base_url}")
    
    # Hardcoded path mappings - could be moved to config file
    path_mappings = {
        "idr0086": "idr0086-miron-micrographs/20200610-ftp/experimentD/Miron_FIB-SEM/Miron_FIB-SEM_processed"
    }
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    for image_id in image_ids:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        try:
            image_name, dataset_dir = fetch_image_name_and_dataset_dir(image_id, api_base_url, dataset_id, path_mappings)
            ftp_path = construct_ftp_path_from_name(dataset_dir, image_name, ftp_root_path)
            ftp_url = f"ftp://{ftp_host}{ftp_path}"
        except Exception as e:
            print(f"❌ Failed to construct FTP path: {e}")
            continue

        # Try FTP download first, with HTTP fallback
        print(f"\n📥 Processing image {image_id}: {image_name}")
        image_path = download_via_ftp(image_id, ftp_path, timestamp, ftp_host, output_dir, max_retries)
        if not image_path:
            print(f"🔄 FTP download failed, attempting HTTP fallback for image {image_id}")
            image_path = download_via_http_fallback(image_id, api_base_url, timestamp, output_dir)
            if not image_path:
                print(f"❌ Both FTP and HTTP download methods failed for image {image_id}")
                continue
            else:
                print(f"✅ HTTP fallback successful for image {image_id}")

        try:
            data = load_image(image_path)
        except Exception as e:
            print(f"⚠️ Skipping image due to load error: {e}")
            continue

        npy_path = os.path.join(output_dir, f"{image_id}_{timestamp}.npy")
        metadata_path = os.path.join(output_dir, f"metadata_{image_id}_{timestamp}.json")

        stub = write_metadata_stub(image_id, image_name, npy_path, metadata_path, ftp_url)

        np.save(npy_path, data)
        print(f"✅ Saved volume as {npy_path}")

        enrich_metadata(metadata_path, stub, data)


def parse_args():
    parser = argparse.ArgumentParser(description='IDR data ingestion')
    parser.add_argument('--config', type=str, default=None,
                        help='Path to configuration file')
    parser.add_argument('--dataset-id', type=str, default=None,
                        help='IDR dataset ID to process')
    parser.add_argument('--image-ids', nargs='+', type=int, default=None,
                        help='List of image IDs to process')
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Initialize config manager
    config_manager = get_config_manager(args.config)
    
    # Override values if provided via command line
    if args.dataset_id:
        config_manager.set('sources.idr.defaults.dataset_id', args.dataset_id)
    if args.image_ids:
        config_manager.set('sources.idr.defaults.image_ids', args.image_ids)
    
    ingest_image_via_ftp(config_manager)


if __name__ == "__main__":
    main()
