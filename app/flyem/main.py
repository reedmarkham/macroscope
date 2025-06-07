import os
import sys
import json
import random
import argparse
import logging
from datetime import datetime
from typing import Tuple, Dict

import numpy as np
import requests

# Add lib directory to path for config_manager import
sys.path.append('/app/lib')
from config_manager import get_config_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)


# ——————————————————————————
# 1) Fetch raw‐grayscale bounds via /api/node/.../info
# ——————————————————————————
def fetch_dataset_bounds(
    server: str,
    uuid: str,
    instance: str
) -> Tuple[Tuple[int,int,int], Tuple[int,int,int]]:
    url = f"{server.rstrip('/')}/api/node/{uuid}/{instance}/info"
    logger.info("Querying node-specific info: GET %s", url)
    resp = requests.get(url)
    resp.raise_for_status()

    inst_info = resp.json()
    logger.info("Raw /info response:\n%s", json.dumps(inst_info, indent=2))

    # 1) "bounds" key?
    if "bounds" in inst_info:
        b0, b1 = inst_info["bounds"]
        start_xyz = tuple(b0)
        stop_xyz  = tuple(b1)
        logger.info("Found 'bounds' -> start=%s, stop=%s", start_xyz, stop_xyz)
        return start_xyz, stop_xyz

    # 2) "Extended.MinZyx" / "Extended.MaxZyx"?
    ext = inst_info.get("Extended", {})
    if ("MinZyx" in ext) and ("MaxZyx" in ext):
        minzyx = ext["MinZyx"]
        maxzyx = ext["MaxZyx"]
        start_xyz = (minzyx[0], minzyx[1], minzyx[2])
        stop_xyz  = (maxzyx[0], maxzyx[1], maxzyx[2])
        logger.info("Found 'Extended.MinZyx' = %s, 'Extended.MaxZyx' = %s", start_xyz, stop_xyz)
        return start_xyz, stop_xyz

    # 3) "Extents.MinPoint" / "Extents.MaxPoint"?
    extents = inst_info.get("Extents", {})
    if ("MinPoint" in extents) and ("MaxPoint" in extents):
        minpt = extents["MinPoint"]
        maxpt = extents["MaxPoint"]
        start_xyz = (minpt[0], minpt[1], minpt[2])
        stop_xyz  = (maxpt[0], maxpt[1], maxpt[2])
        logger.info("Found 'Extents.MinPoint' = %s, 'Extents.MaxPoint' = %s", start_xyz, stop_xyz)
        return start_xyz, stop_xyz

    # 4) "Size" fallback (X,Y,Z → (0,0,0)-(Z-1,Y-1,X-1))
    if "Size" in inst_info:
        X, Y, Z = inst_info["Size"][:3]
        start_xyz = (0, 0, 0)
        stop_xyz  = (Z - 1, Y - 1, X - 1)
        logger.info("Found 'Size' = %s; assuming start=%s, stop=%s", inst_info['Size'][:3], start_xyz, stop_xyz)
        return start_xyz, stop_xyz

    raise RuntimeError(f"Could not find any recognized bounds in /info JSON for instance '{instance}'.")


def random_origin(
    bounds: Tuple[Tuple[int,int,int], Tuple[int,int,int]],
    crop_size: Tuple[int,int,int]
) -> Tuple[int,int,int]:
    (z0, y0, x0), (z1, y1, x1) = bounds
    dz, dy, dx = crop_size

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
    logger.info("DVID GET %s", url)
    resp = requests.get(url)
    resp.raise_for_status()
    arr = np.frombuffer(resp.content, dtype=np.uint8)
    expected_size = dx * dy * dz
    if arr.size != expected_size:
        logger.error("3D block has size %d (expected %d)", arr.size, expected_size)
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
    bounds: Tuple[Tuple[int,int,int], Tuple[int,int,int]],
    output_dir: str
) -> Dict:
    name = os.path.splitext(os.path.basename(vol_path))[0]
    return {
        "source": "flyem",
        "source_id": uuid,
        "description": f"random crop at {crop_origin}",
        "volume_shape": list(crop_shape),
        "voxel_size_nm": None,
        "download_url": server,
        "local_paths": {
            "volume": vol_path,
            "metadata": os.path.join(output_dir, f"{name}_metadata.json")
        },
        "additional_metadata": {
            "dataset_bounds": [list(bounds[0]), list(bounds[1])],
            "crop_origin":   list(crop_origin),
            "crop_size":     list(crop_shape)
        },
        "timestamp": timestamp
    }


def save(volume: np.ndarray, meta: Dict, name: str, output_dir: str) -> None:
    vol_path = os.path.join(output_dir, f"{name}.npy")
    np.save(vol_path, volume)

    meta["local_paths"]["volume"] = vol_path
    meta_path = meta["local_paths"]["metadata"]
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

    logger.info("Saved 3D volume -> %s", vol_path)
    logger.info("Saved metadata -> %s", meta_path)


def fetch_random_crop(config):
    # Get configuration values
    dvid_server = config.get('sources.flyem.base_urls.neuroglancer', 'http://hemibrain-dvid.janelia.org')
    uuid = config.get('sources.flyem.defaults.uuid', 'a89eb3af216a46cdba81204d8f954786')
    instance = config.get('sources.flyem.defaults.instance', 'grayscale')
    crop_size = tuple(config.get('sources.flyem.defaults.crop_size', [1000, 1000, 1000]))
    output_dir = os.environ.get('EM_DATA_DIR', config.get('sources.flyem.output_dir', './data/flyem'))
    random_seed = config.get('sources.flyem.defaults.random_seed')
    
    # Set random seed if provided
    if random_seed is not None:
        random.seed(random_seed)
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    try:
        bounds = fetch_dataset_bounds(dvid_server, uuid, instance)
    except Exception as e:
        logger.error("Failed to fetch dataset bounds: %s", e)
        return

    origin = random_origin(bounds, crop_size)
    logger.info("Fetching raw-grayscale: origin=%s, size=%s", origin, crop_size)

    ts   = datetime.now().strftime("%Y%m%d_%H%M%S")
    name = f"crop_z{origin[0]}_y{origin[1]}_x{origin[2]}_{ts}"

    try:
        volume = fetch_gray3d_raw(
            dvid_server, uuid, instance,
            origin, crop_size
        )
    except Exception as e:
        logger.error("Error during DVID raw-fetch: %s", e)
        return

    meta = build_metadata(
        server=     dvid_server,
        uuid=       uuid,
        crop_origin= origin,
        crop_shape=  crop_size,
        vol_path=    os.path.join(output_dir, f"{name}.npy"),
        timestamp=   ts,
        bounds=      bounds,
        output_dir=  output_dir
    )
    save(volume, meta, name, output_dir)


def parse_args():
    parser = argparse.ArgumentParser(description='FlyEM DVID data ingestion')
    parser.add_argument('--config', type=str, default=None,
                        help='Path to configuration file')
    parser.add_argument('--uuid', type=str, default=None,
                        help='DVID UUID to process')
    parser.add_argument('--crop-size', nargs=3, type=int, default=None,
                        help='Crop size as three integers (z y x)')
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Initialize config manager
    config_manager = get_config_manager(args.config)
    
    # Override values if provided via command line
    if args.uuid:
        config_manager.set('sources.flyem.defaults.uuid', args.uuid)
    if args.crop_size:
        config_manager.set('sources.flyem.defaults.crop_size', args.crop_size)
    
    output_dir = config_manager.get('sources.flyem.output_dir', './data/flyem')
    logger.info("Ensured output directory exists: %s", output_dir)
    fetch_random_crop(config_manager)


if __name__ == "__main__":
    main()
