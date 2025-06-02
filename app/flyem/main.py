import os
import json
import random
from datetime import datetime
from typing import Tuple, Dict


import numpy as np
from neuclease.dvid import fetch_repo_info, fetch_gray3d


OUTPUT_DIR = "dvid_crops"
os.makedirs(OUTPUT_DIR, exist_ok=True)


CROP_SIZE = (1000, 1000, 1000) # (Z, Y, X)
DVID_SERVER = "http://hemibrain-dvid.janelia.org"
UUID        = "be657e3e00df40f2904282648a330bbc"
INSTANCE    = os.environ.get("GRAYSCALE_INSTANCE", "grayscale")


# Full volume bounds for the given grayscale instance
def fetch_dataset_bounds(server: str, uuid: str, instance: str) -> Tuple[Tuple[int,int,int], Tuple[int,int,int]]:
    info = fetch_repo_info(server, uuid)
    start_xyz, stop_xyz = info['grayscale'][instance]['bounds']
    return tuple(start_xyz), tuple(stop_xyz)


def random_origin(bounds: Tuple[Tuple[int,int,int], Tuple[int,int,int]]) -> Tuple[int,int,int]:
    (z0,y0,x0),(z1,y1,x1) = bounds
    dz,dy,dx = CROP_SIZE
    if z1-z0 < dz or y1-y0 < dy or x1-x0 < dx:
        raise ValueError("CROP_SIZE exceeds dataset extents.")
    return (random.randint(z0, z1-dz),
            random.randint(y0, y1-dy),
            random.randint(x0, x1-dx))


def fetch_crop(server: str, uuid: str, instance: str, origin: Tuple[int,int,int]) -> np.ndarray:
    z,y,x = origin
    dz,dy,dx = CROP_SIZE
    print(f"📥 Fetching volume ({z}:{z+dz}, {y}:{y+dy}, {x}:{x+dx})...")
    return fetch_gray3d(server, uuid, instance, (z,y,x), CROP_SIZE)


def build_metadata(server, uuid, crop_origin, crop_shape, vol_path, timestamp, bounds)->Dict:
    name = os.path.splitext(os.path.basename(vol_path))[0]
    return {
        "source": "neuclease",
        "uuid": uuid,
        "description": f"random crop at {crop_origin}",
        "volume_shape": list(crop_shape),
        "voxel_size_nm": None,           # populate if desired
        "download_url": server,
        "local_paths": {
            "volume": vol_path,
            "metadata": os.path.join(OUTPUT_DIR, f"{name}_metadata.json")
        },
        "additional_metadata": {
            "dataset_bounds": bounds,
            "crop_origin": list(crop_origin),
            "crop_size": list(crop_shape)
        }
    }


def save(volume: np.ndarray, meta: Dict, name: str):
    vol_path = os.path.join(OUTPUT_DIR, f"{name}.npy")
    np.save(vol_path, volume)
    meta["local_paths"]["volume"] = vol_path
    with open(meta["local_paths"]["metadata"], "w") as f:
        json.dump(meta, f, indent=2)
    print(f"✅ Saved volume → {vol_path}")
    print(f"✅ Saved metadata → {meta['local_paths']['metadata']}")


def fetch_random_crop():
    bounds = fetch_dataset_bounds(DVID_SERVER, UUID, INSTANCE)
    origin = random_origin(bounds)

    volume = fetch_crop(DVID_SERVER, UUID, INSTANCE, origin)

    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"crop_z{origin[0]}_y{origin[1]}_x{origin[2]}_{ts}"

    meta = build_metadata(DVID_SERVER, UUID, origin, CROP_SIZE,
                          os.path.join(OUTPUT_DIR, f"{name}.npy"),
                          ts, bounds)

    save(volume, meta, name)


if __name__ == "__main__":
    fetch_random_crop()
