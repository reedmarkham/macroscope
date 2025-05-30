import os
import json
from datetime import datetime

import requests
import tifffile
import numpy as np
from tqdm import tqdm


URL = "https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/volumedata.tif"
OUTPUT_DIR = "epfl_em_data"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def download_tif(url: str, output_path: str):
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


def save_volume(volume: np.ndarray, output_path: str):
    np.save(output_path, volume)
    print(f"Saved volume as .npy to {output_path}")


def write_metadata(volume: np.ndarray, tif_path: str, npy_path: str, timestamp: str):
    metadata_filename = f"metadata_{timestamp}.json"
    metadata_path = os.path.join(OUTPUT_DIR, metadata_filename)
    metadata = {
        "source": "epfl",
        "source_id": "EPFL-CA1-HIPPOCAMPUS",
        "description": "5x5x5µm section from CA1 hippocampus region",
        "volume_shape": list(volume.shape),
        "voxel_size_nm": [5, 5, 5],
        "download_url": URL,
        "local_paths": {
            "volume": npy_path,
            "raw": tif_path,
            "metadata": metadata_path
        }
    }
    
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to {metadata_path}")


def ingest_epfl_tif():
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    tif_filename = f"volumedata_{timestamp}.tif"
    npy_filename = f"volumedata_{timestamp}.npy"
    tif_path = os.path.join(OUTPUT_DIR, tif_filename)
    npy_path = os.path.join(OUTPUT_DIR, npy_filename)

    download_tif(URL, tif_path)
    volume = load_volume(tif_path)
    save_volume(volume, npy_path)
    write_metadata(volume, tif_path, npy_path, timestamp)


if __name__ == "__main__":
    ingest_epfl_tif()
