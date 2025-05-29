import os
import json
from datetime import datetime  # Add this import

import requests
import tifffile
import numpy as np

OUTPUT_DIR = 'idr_volumes'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def establish_session():
    session = requests.Session()
    index_url = "https://idr.openmicroscopy.org/webclient/?experimenter=-1"
    response = session.get(index_url)
    response.raise_for_status()
    return session

def fetch_metadata(session, image_id):
    metadata_url = f"https://idr.openmicroscopy.org/webclient/api/images/{image_id}/"
    response = session.get(metadata_url)
    response.raise_for_status()
    metadata = response.json()
    print(f"Image Metadata:\nName: {metadata['name']}\nSize: {metadata['sizeX']}x{metadata['sizeY']}x{metadata['sizeZ']}")
    return metadata

def download_image(session, image_id, output_dir=OUTPUT_DIR, timestamp=None):
    image_url = f"https://idr.openmicroscopy.org/webclient/render_image_download/{image_id}/?format=ome.tiff"
    if timestamp is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    image_path = os.path.join(output_dir, f"{image_id}_{timestamp}.ome.tiff")
    response = session.get(image_url, stream=True)
    with open(image_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"Downloaded image to {image_path}")
    return image_path

def load_image(image_path):
    with tifffile.TiffFile(image_path) as tif:
        image_data = tif.asarray()
    print(f"Loaded image data with shape: {image_data.shape}")
    return image_data

def write_metadata(metadata, image_id, shape, volume_path, timestamp):
    metadata_filename = f"metadata_{image_id}_{timestamp}.json"
    metadata_path = os.path.join(OUTPUT_DIR, metadata_filename)
    metadata_out = {
        "source": "idr",
        "source_id": f"IDR-{image_id}",
        "description": metadata.get("name", ""),
        "volume_shape": shape,
        "voxel_size_nm": None,
        "modality": "light microscopy",
        "download_url": f"https://idr.openmicroscopy.org/webclient/render_image_download/{image_id}/?format=ome.tiff",
        "local_paths": {
            "volume": volume_path,
            "metadata": metadata_path
        },
        "additional_metadata": metadata
    }

    with open(metadata_path, "w") as f:
        json.dump(metadata_out, f, indent=2)
    print(f"Saved metadata to {metadata_path}")

def ingest_image(image_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    session = establish_session()
    metadata = fetch_metadata(session, image_id)
    image_path = download_image(session, image_id, timestamp=timestamp)
    image_data = load_image(image_path)

    volume_path = os.path.join(OUTPUT_DIR, f"{image_id}_{timestamp}.npy")
    np.save(volume_path, image_data)
    print(f"Saved full image volume to {volume_path}")

    write_metadata(metadata, image_id, list(image_data.shape), volume_path, timestamp)

if __name__ == "__main__":
    ingest_image(9846137)  # Example image ID
