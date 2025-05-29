import os
import json
from datetime import datetime
from ftplib import FTP

import numpy as np
import tifffile
import requests

OUTPUT_DIR = os.environ.get("IDR_OUTPUT_DIR", "idr_volumes")
os.makedirs(OUTPUT_DIR, exist_ok=True)

FTP_HOST = "ftp.ebi.ac.uk"
FTP_ROOT_PATH = "/pub/databases/IDR"

HARDCODED_PATHS = {
    "idr0086": "idr0086-miron-micrographs/20200610-ftp/experimentD/Miron_FIB-SEM/Miron_FIB-SEM_processed"
}


def get_image_ids_from_dataset(dataset_id):
    url = f"https://idr.openmicroscopy.org/api/v0/m/images/?dataset={dataset_id}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]
    return [(img["@id"], img.get("Name", f"Image {img['@id']}")) for img in data]


def fetch_image_name_and_dataset_dir(image_id):
    url = f"https://idr.openmicroscopy.org/api/v0/m/images/{image_id}/"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]  # FIX: access "data" inside JSON

    image_name = data.get("Name")
    if not image_name:
        raise ValueError("No image name found in metadata.")

    dataset_dir = HARDCODED_PATHS.get("idr0086")
    if not dataset_dir:
        raise ValueError("No hardcoded dataset path found for idr0086.")

    return image_name, dataset_dir


def construct_ftp_path_from_name(dataset_dir, image_name):
    return f"{FTP_ROOT_PATH}/{dataset_dir}/{image_name}"


def download_via_ftp(image_id, ftp_path, timestamp):
    ftp = FTP(FTP_HOST)
    ftp.login()
    local_path = os.path.join(OUTPUT_DIR, f"{image_id}_{timestamp}.tif")
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


def load_image(image_path):
    with tifffile.TiffFile(image_path) as tif:
        data = tif.asarray()
    print(f"✅ Loaded image with shape: {data.shape}")
    return data


def write_metadata(image_id, name, shape, volume_path, timestamp, ftp_url):
    metadata_path = os.path.join(OUTPUT_DIR, f"metadata_{image_id}_{timestamp}.json")
    metadata = {
        "source": "idr",
        "source_id": f"IDR-{image_id}",
        "description": name,
        "volume_shape": shape,
        "voxel_size_nm": None,
        "modality": "light microscopy",
        "download_url": ftp_url,
        "local_paths": {
            "volume": volume_path,
            "metadata": metadata_path
        }
    }
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"✅ Wrote metadata to {metadata_path}")


def ingest_image_via_ftp(image_id):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    try:
        image_name, dataset_dir = fetch_image_name_and_dataset_dir(image_id)
        ftp_path = construct_ftp_path_from_name(dataset_dir, image_name)
        ftp_url = f"ftp://{FTP_HOST}{ftp_path}"
    except Exception as e:
        print(f"❌ Failed to construct FTP path: {e}")
        return

    image_path = download_via_ftp(image_id, ftp_path, timestamp)
    if not image_path:
        return

    try:
        data = load_image(image_path)
    except Exception as e:
        print(f"⚠️ Skipping image due to load error: {e}")
        return

    npy_path = os.path.join(OUTPUT_DIR, f"{image_id}_{timestamp}.npy")
    np.save(npy_path, data)
    print(f"✅ Saved volume as {npy_path}")

    write_metadata(image_id, image_name, list(data.shape), npy_path, timestamp, ftp_url)


if __name__ == "__main__":
    IMAGE_IDS = [9846137]  # Example image ID from idr0086
    for image_id in IMAGE_IDS:
        ingest_image_via_ftp(image_id)
