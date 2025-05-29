import os
import json
import random

import numpy as np
from neuprint import Client, fetch_volume_roi

OUTPUT_DIR = "neuprint_crops"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CROP_SIZE = (1000, 1000, 1000)  # Z, Y, X

def fetch_random_crop(server, dataset, token):
    client = Client(server, dataset=dataset, token=token)

    # Fetch bounding box for main ROI (assume whole brain)
    bounds = client.fetch_roi_bounds()
    if not bounds:
        raise RuntimeError("No ROI bounds found in the dataset")

    roi_name, roi_bounds = next(iter(bounds.items()))
    print(f"Using ROI: {roi_name} with bounds {roi_bounds}")

    (z0, y0, x0), (z1, y1, x1) = roi_bounds
    z_max = z1 - CROP_SIZE[0]
    y_max = y1 - CROP_SIZE[1]
    x_max = x1 - CROP_SIZE[2]

    z = random.randint(z0, z_max)
    y = random.randint(y0, y_max)
    x = random.randint(x0, x_max)

    print(f"Fetching crop at (z={z}, y={y}, x={x}) of size {CROP_SIZE}")
    volume = fetch_volume_roi(client, roi_name, (z, y, x), CROP_SIZE)

    crop_name = f"crop_z{z}_y{y}_x{x}"
    volume_path = os.path.join(OUTPUT_DIR, f"{crop_name}.npy")
    np.save(volume_path, volume)
    print(f"Saved crop to {volume_path}")

    metadata = {
        "source": "neuprint",
        "source_id": dataset,
        "description": f"{roi_name} crop at (z={z}, y={y}, x={x})",
        "volume_shape": list(volume.shape),
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
            "crop_origin": [z, y, x],
            "crop_size": CROP_SIZE
        }
    }

    metadata_path = metadata["local_paths"]["metadata"]
    with open(metadata_path, "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to {metadata_path}")

if __name__ == "__main__":
    server = os.environ.get("NEUPRINT_SERVER", "https://neuprint.janelia.org")
    dataset = os.environ.get("NEUPRINT_DATASET", "hemibrain:v1.2.1")

    token = os.environ.get("NEUPRINT_TOKEN")
    if not token:
        raise ValueError("Please set NEUPRINT_TOKEN in your environment.")

    fetch_random_crop(server, dataset, token)
