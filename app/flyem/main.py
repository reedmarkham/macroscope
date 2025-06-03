import os
import json
import random
from datetime import datetime
from typing import Tuple, Dict

import numpy as np
import requests

# ——————————————————————————
# CONFIG / CONSTANTS
# ——————————————————————————
OUTPUT_DIR   = "dvid_crops"
os.makedirs(OUTPUT_DIR, exist_ok=True)

CROP_SIZE    = (1000, 1000, 1000)    # (dz, dy, dx)
DVID_SERVER  = "http://hemibrain-dvid.janelia.org"
UUID         = "a89eb3af216a46cdba81204d8f954786"
INSTANCE     = os.environ.get("GRAYSCALE_INSTANCE", "grayscale")


def log(msg: str, level: str = "INFO") -> None:
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"{ts} [{level}] {msg}")


# ——————————————————————————
# 1) Fetch raw‐grayscale bounds via /api/node/.../info
# ——————————————————————————
def fetch_dataset_bounds(
    server: str,
    uuid: str,
    instance: str
) -> Tuple[Tuple[int,int,int], Tuple[int,int,int]]:
    url = f"{server.rstrip('/')}/api/node/{uuid}/{instance}/info"
    log(f"Querying node‐specific info: GET {url}")
    resp = requests.get(url)
    resp.raise_for_status()

    inst_info = resp.json()
    log(f"Raw /info response:\n{json.dumps(inst_info, indent=2)}")

    # 1) "bounds" key?
    if "bounds" in inst_info:
        b0, b1 = inst_info["bounds"]
        start_xyz = tuple(b0)
        stop_xyz  = tuple(b1)
        log(f"Found 'bounds' → start={start_xyz}, stop={stop_xyz}")
        return start_xyz, stop_xyz

    # 2) "Extended.MinZyx" / "Extended.MaxZyx"?
    ext = inst_info.get("Extended", {})
    if ("MinZyx" in ext) and ("MaxZyx" in ext):
        minzyx = ext["MinZyx"]
        maxzyx = ext["MaxZyx"]
        start_xyz = (minzyx[0], minzyx[1], minzyx[2])
        stop_xyz  = (maxzyx[0], maxzyx[1], maxzyx[2])
        log(f"Found 'Extended.MinZyx' = {start_xyz}, 'Extended.MaxZyx' = {stop_xyz}")
        return start_xyz, stop_xyz

    # 3) "Extents.MinPoint" / "Extents.MaxPoint"?
    extents = inst_info.get("Extents", {})
    if ("MinPoint" in extents) and ("MaxPoint" in extents):
        minpt = extents["MinPoint"]
        maxpt = extents["MaxPoint"]
        start_xyz = (minpt[0], minpt[1], minpt[2])
        stop_xyz  = (maxpt[0], maxpt[1], maxpt[2])
        log(f"Found 'Extents.MinPoint' = {start_xyz}, 'Extents.MaxPoint' = {stop_xyz}")
        return start_xyz, stop_xyz

    # 4) "Size" fallback (X,Y,Z → (0,0,0)-(Z-1,Y-1,X-1))
    if "Size" in inst_info:
        X, Y, Z = inst_info["Size"][:3]
        start_xyz = (0, 0, 0)
        stop_xyz  = (Z - 1, Y - 1, X - 1)
        log(f"Found 'Size' = {inst_info['Size'][:3]}; assuming start={start_xyz}, stop={stop_xyz}")
        return start_xyz, stop_xyz

    raise RuntimeError(f"Could not find any recognized bounds in /info JSON for instance '{instance}'.")


def random_origin(
    bounds: Tuple[Tuple[int,int,int], Tuple[int,int,int]]
) -> Tuple[int,int,int]:
    (z0, y0, x0), (z1, y1, x1) = bounds
    dz, dy, dx = CROP_SIZE

    # +1 because bounds are inclusive
    if (z1 - z0 + 1) < dz or (y1 - y0 + 1) < dy or (x1 - x0 + 1) < dx:
        raise ValueError("CROP_SIZE exceeds dataset extents.")

    z = random.randint(z0, z1 - dz + 1)
    y = random.randint(y0, y1 - dy + 1)
    x = random.randint(x0, x1 - dx + 1)
    return (z, y, x)


# ——————————————————————————
# 2) Fetch 3D grayscale via /raw/0_1_2 API (single call)
# ——————————————————————————
def fetch_gray3d_raw(
    server: str,
    uuid: str,
    instance: str,
    origin: Tuple[int, int, int],
    size: Tuple[int, int, int]
) -> np.ndarray:
    """
    Fetch a 3D grayscale crop using the DVID /raw/0_1_2 API.
    Returns a numpy array of shape (dz, dy, dx) in ZYX order.
    """
    dz, dy, dx = size
    z0, y0, x0 = origin
    url = (
        f"{server.rstrip('/')}/api/node/{uuid}/{instance}/raw/0_1_2/"
        f"{dx}_{dy}_{dz}/{x0}_{y0}_{z0}"
    )
    log(f"📥 DVID GET {url}")
    resp = requests.get(url)
    resp.raise_for_status()
    arr = np.frombuffer(resp.content, dtype=np.uint8)
    expected_size = dx * dy * dz
    if arr.size != expected_size:
        log(
            f"[ERROR] 3D block has size {arr.size} (expected {expected_size}).",
            level="ERROR"
        )
        raise ValueError(f"3D block has size {arr.size} (expected {expected_size})")
    # DVID returns ZYX order, X fastest
    arr = arr.reshape((dz, dy, dx))
    return arr


def build_metadata(
    server: str,
    uuid: str,
    crop_origin: Tuple[int,int,int],
    crop_shape: Tuple[int,int,int],
    vol_path: str,
    timestamp: str,
    bounds: Tuple[Tuple[int,int,int], Tuple[int,int,int]]
) -> Dict:
    name = os.path.splitext(os.path.basename(vol_path))[0]
    return {
        "source": "direct-DVID",
        "uuid": uuid,
        "description": f"random crop at {crop_origin}",
        "volume_shape": list(crop_shape),
        "voxel_size_nm": None,
        "download_url": server,
        "local_paths": {
            "volume": vol_path,
            "metadata": os.path.join(OUTPUT_DIR, f"{name}_metadata.json")
        },
        "additional_metadata": {
            "dataset_bounds": [list(bounds[0]), list(bounds[1])],
            "crop_origin":   list(crop_origin),
            "crop_size":     list(crop_shape)
        },
        "timestamp": timestamp
    }


def save(volume: np.ndarray, meta: Dict, name: str) -> None:
    vol_path = os.path.join(OUTPUT_DIR, f"{name}.npy")
    np.save(vol_path, volume)

    meta["local_paths"]["volume"] = vol_path
    meta_path = meta["local_paths"]["metadata"]
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    log(f"✅ Saved 3D volume → {vol_path}")
    log(f"✅ Saved metadata → {meta_path}")


# ——————————————————————————
# 3) Top‐level: fetch raw‐grayscale crop only
# ——————————————————————————
def fetch_random_crop():
    try:
        bounds = fetch_dataset_bounds(DVID_SERVER, UUID, INSTANCE)
    except Exception as e:
        log(f"Failed to fetch dataset bounds: {e}", level="ERROR")
        return

    origin = random_origin(bounds)
    log(f"📥 Fetching raw‐grayscale: origin={origin}, size={CROP_SIZE}")

    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"crop_z{origin[0]}_y{origin[1]}_x{origin[2]}_{ts}"

    try:
        volume = fetch_gray3d_raw(
            DVID_SERVER, UUID, INSTANCE,
            origin, CROP_SIZE
        )
    except Exception as e:
        log(f"Error during DVID raw‐fetch: {e}", level="ERROR")
        return

    meta = build_metadata(
        server=     DVID_SERVER,
        uuid=       UUID,
        crop_origin= origin,
        crop_shape=  CROP_SIZE,
        vol_path=    os.path.join(OUTPUT_DIR, f"{name}.npy"),
        timestamp=   ts,
        bounds=      bounds
    )
    save(volume, meta, name)


if __name__ == "__main__":
    log(f"Ensured output directory exists: '{OUTPUT_DIR}'")
    fetch_random_crop()
