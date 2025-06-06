import os
import sys
import json
import argparse
import hashlib
from datetime import datetime
from ftplib import FTP
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


def download_via_ftp(image_id: Union[str, int], ftp_path: str, timestamp: str, ftp_host: str, output_dir: str) -> Optional[str]:
    ftp = FTP(ftp_host)
    ftp.login()
    local_path = os.path.join(output_dir, f"{image_id}_{timestamp}.tif")
    try:
        with open(local_path, "wb") as f:
            print(f"📥 FTP downloading {ftp_path}")
            ftp.retrbinary(f"RETR {ftp_path}", f.write)
    except Exception as e:
        print(f"❌ FTP download failed: {e}")
        return None
    finally:
        ftp.quit()
    print(f"✅ Downloaded via FTP to {local_path}")
    return local_path


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

        image_path = download_via_ftp(image_id, ftp_path, timestamp, ftp_host, output_dir)
        if not image_path:
            continue

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
