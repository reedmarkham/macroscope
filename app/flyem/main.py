import os
import json
import random
from datetime import datetime
from typing import Tuple, Dict

import numpy as np
from neuprint import Client
from cloudvolume import CloudVolume

OUTPUT_DIR = "neuprint_crops"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CROP_SIZE = (1000, 1000, 1000)  # Z, Y, X
MIP_LEVEL = 0

CLOUDVOLUME_URL = os.environ.get(
    "CLOUDVOLUME_URL",
    "gs://neuroglancer-janelia-flyem-hemibrain/emdata/raw/jpeg"
)


def fetch_roi_bounds(client: Client) -> Dict[str, Tuple[Tuple[int, int, int], Tuple[int, int, int]]]:
    """Query the neuPrint server for ROI bounds using Cypher."""
    query = """
    MATCH (n:ROIInfo)
    RETURN n.roiInfo AS info
    """
    df = client.fetch_custom(query)
    roi_info = json.loads(df.iloc[0]['info'])

    bounds = {}
    for roi, info in roi_info.items():
        z_range = info['z']
        y_range = info['y']
        x_range = info['x']
        bounds[roi] = ((z_range[0], y_range[0], x_range[0]), (z_range[1], y_range[1], x_range[1]))
    return bounds


def get_random_crop_origin(bounds: Tuple[Tuple[int, int, int], Tuple[int, int, int]]) -> Tuple[int, int, int]:
    (z0, y0, x0), (z1, y1, x1) = bounds
    return (
        random.randint(z0, z1 - CROP_SIZE[0]),
        random.randint(y0, y1 - CROP_SIZE[1]),
        random.randint(x0, x1 - CROP_SIZE[2])
    )


def fetch_crop_volume(cloudvolume_url: str, origin: Tuple[int, int, int]) -> np.ndarray:
    cv = CloudVolume(cloudvolume_url, mip=MIP_LEVEL, progress=True, parallel=True)
    z, y, x = origin
    dz, dy, dx = CROP_SIZE
    return cv[x:x+dx, y:y+dy, z:z+dz].squeeze()


def build_metadata(server, dataset, roi_name, roi_bounds, crop_origin, crop_shape, volume_path, timestamp) -> Dict:
    crop_name = os.path.splitext(os.path.basename(volume_path))[0]
    return {
        "source": "neuprint+cloudvolume",
        "source_id": dataset,
        "description": f"{roi_name} crop at {crop_origin}",
        "volume_shape": list(crop_shape),
        "voxel_size_nm": None,
        "modality": "connectomics",
        "download_url": server,
        "local_paths": {
            "volume": volume_path,
            "metadata": os.path.join(OUTPUT_DIR, f"{crop_name}_metadata.json")
        },
        "additional_metadata": {
            "roi_name": roi_name,
            "bounds": roi_bounds,
            "crop_origin": list(crop_origin),
            "crop_size": list(crop_shape)
        }
    }


def save_crop(volume: np.ndarray, metadata: Dict, crop_name: str):
    volume_path = os.path.join(OUTPUT_DIR, f"{crop_name}.npy")
    np.save(volume_path, volume)
    metadata["local_paths"]["volume"] = volume_path

    metadata_path = metadata["local_paths"]["metadata"]
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)

    print(f"✅ Saved volume to {volume_path}")
    print(f"✅ Saved metadata to {metadata_path}")


def fetch_random_crop(server: str, dataset: str, token: str):
    client = Client(server, dataset=dataset, token=token)
    bounds = fetch_roi_bounds(client)
    if not bounds:
        raise RuntimeError("No ROI bounds found.")

    roi_name, roi_bounds = next(iter(bounds.items()))
    print(f"🎯 Using ROI: {roi_name} with bounds: {roi_bounds}")

    crop_origin = get_random_crop_origin(roi_bounds)
    print(f"📦 Sampling crop at (z, y, x): {crop_origin}, size: {CROP_SIZE}")

    volume = fetch_crop_volume(CLOUDVOLUME_URL, crop_origin)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    crop_name = f"crop_z{crop_origin[0]}_y{crop_origin[1]}_x{crop_origin[2]}_{timestamp}"

    metadata = build_metadata(
        server, dataset, roi_name, roi_bounds,
        crop_origin, CROP_SIZE,
        os.path.join(OUTPUT_DIR, f"{crop_name}.npy"),
        timestamp
    )

    save_crop(volume, metadata, crop_name)


if __name__ == "__main__":
    server = "https://neuprint.janelia.org"
    dataset = "hemibrain:v1.2.1"
    token = os.environ.get("NEUPRINT_TOKEN")

    if not token:
        raise ValueError("❌ Please set NEUPRINT_TOKEN in your environment.")

    fetch_random_crop(server, dataset, token)
