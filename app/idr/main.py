import os
import json
import hashlib
from datetime import datetime
from ftplib import FTP
from typing import Any, Dict, List, Optional, Tuple, Union

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


def get_image_ids_from_dataset(dataset_id: Union[str, int]) -> List[Tuple[int, str]]:
    url = f"https://idr.openmicroscopy.org/api/v0/m/images/?dataset={dataset_id}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]
    return [(img["@id"], img.get("Name", f"Image {img['@id']}")) for img in data]


def fetch_image_name_and_dataset_dir(image_id: Union[str, int]) -> Tuple[str, str]:
    url = f"https://idr.openmicroscopy.org/api/v0/m/images/{image_id}/"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()["data"]

    image_name: Optional[str] = data.get("Name")
    if not image_name:
        raise ValueError("No image name found in metadata.")

    dataset_dir: Optional[str] = HARDCODED_PATHS.get("idr0086")
    if not dataset_dir:
        raise ValueError("No hardcoded dataset path found for idr0086.")

    return image_name, dataset_dir


def construct_ftp_path_from_name(dataset_dir: str, image_name: str) -> str:
    return f"{FTP_ROOT_PATH}/{dataset_dir}/{image_name}"


def download_via_ftp(image_id: Union[str, int], ftp_path: str, timestamp: str) -> Optional[str]:
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


def ingest_image_via_ftp(image_id: Union[str, int]) -> None:
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
    metadata_path = os.path.join(OUTPUT_DIR, f"metadata_{image_id}_{timestamp}.json")

    stub = write_metadata_stub(image_id, image_name, npy_path, metadata_path, ftp_url)

    np.save(npy_path, data)
    print(f"✅ Saved volume as {npy_path}")

    enrich_metadata(metadata_path, stub, data)


if __name__ == "__main__":
    IMAGE_IDS: List[int] = [9846137]  # Example image ID from idr0086
    for image_id in IMAGE_IDS:
        ingest_image_via_ftp(image_id)
