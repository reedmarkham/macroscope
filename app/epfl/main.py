import os
import sys
import json
import argparse
from datetime import datetime

import requests
import tifffile
import numpy as np
from tqdm import tqdm

# Add lib directory to path for config_manager import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
from config_manager import get_config_manager


def download_tif(url: str, output_path: str) -> None:
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in tqdm(response.iter_content(chunk_size=8 * 1024 * 1024), desc="Downloading TIFF"):
            f.write(chunk)
    print(f"Downloaded TIFF to {output_path}")


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
