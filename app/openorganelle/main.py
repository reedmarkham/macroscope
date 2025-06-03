import os
import json
import time
import hashlib
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import zarr
import s3fs
import numpy as np
import dask.array as da
from tqdm import tqdm


S3_URI = "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr"
KNOWN_ZARR_PATH = "recon-2/em"
OUTPUT_DIR = "./zarr_volume"
MAX_WORKERS = 4


def load_zarr_arrays_from_s3(bucket_uri: str, internal_path: str) -> dict:
    if not bucket_uri.startswith("s3://"):
        raise ValueError("URI must start with s3://")

    print(f"🔗 Connecting to S3 bucket: {bucket_uri}")
    start = time.perf_counter()

    s3 = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)

    try:
        zgroup = zarr.open_consolidated(store)
        print("✅ Loaded Zarr using consolidated metadata")
    except (zarr.errors.MetadataError, KeyError) as e:
        print(f"⚠️ Consolidated metadata not found, falling back to open_group: {e}")
        zgroup = zarr.open_group(store)

    try:
        subgroup = zgroup[internal_path]
        print(f"📂 Accessed internal path: {internal_path}")
    except KeyError:
        raise ValueError(f"❌ Subgroup '{internal_path}' not found in Zarr store")

    def load_subgroup_arrays(subname):
        result = {}
        subsubgroup = subgroup[subname]
        for arr_key in subsubgroup.array_keys():
            full_key = f"{subname}/{arr_key}"
            result[full_key] = da.from_zarr(subsubgroup[arr_key])
        return result

    arrays = {}
    group_keys = list(subgroup.group_keys())
    print(f"🔍 Found {len(group_keys)} subgroup(s); launching parallel metadata loading")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(load_subgroup_arrays, name): name for name in group_keys}
        for future in tqdm(as_completed(futures), total=len(futures), desc="📥 Loading arrays"):
            name = futures[future]
            try:
                result = future.result()
                arrays.update(result)
            except Exception as e:
                print(f"❌ Error loading subgroup '{name}': {e}")
                traceback.print_exc()

    if not arrays:
        raise ValueError(f"❌ No arrays found under '{internal_path}'")

    elapsed = time.perf_counter() - start
    print(f"🧩 Total arrays discovered: {len(arrays)} in {elapsed:.2f}s")
    return arrays


def summarize_data(data: da.Array) -> dict:
    shape = data.shape
    dtype = str(data.dtype)
    chunk_size = data.chunksize
    mean_val = data.mean().compute()

    return {
        "volume_shape": shape,
        "dtype": dtype,
        "chunk_size": chunk_size,
        "global_mean": float(mean_val)
    }


def write_metadata_stub(name, npy_path, metadata_path, s3_uri, internal_path) -> None:
    return {
        "source": "openorganelle",
        "source_id": os.path.basename(s3_uri).replace(".zarr", ""),
        "description": f"Array '{name}' from OpenOrganelle Zarr S3 store",
        "download_url": s3_uri,
        "internal_zarr_path": f"{internal_path}/{name}",
        "imaging_start_date": "Mon Mar 09 2015",
        "voxel_size_nm": {"x": 4.0, "y": 4.0, "z": 2.96},
        "dimensions_nm": {"x": 10384, "y": 10080, "z": 1669.44},
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "local_paths": {
            "volume": npy_path,
            "metadata": metadata_path
        },
        "status": "saving-data"
    }


def save_metadata_atomically(metadata_path: str, data: dict) -> None:
    tmp_path = metadata_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, metadata_path)


def save_volume_and_metadata(name: str, data: da.Array, output_dir: str, s3_uri: str, internal_path: str, timestamp: str) -> str:
    try:
        safe_name = name.replace("/", "_")
        volume_path = os.path.join(output_dir, f"{safe_name}_{timestamp}.npy")
        metadata_path = os.path.join(output_dir, f"metadata_{safe_name}_{timestamp}.json")

        # Step 1: Write stub first
        stub = write_metadata_stub(name, volume_path, metadata_path, s3_uri, internal_path)
        save_metadata_atomically(metadata_path, stub)

        # Step 2: Compute and save volume
        compute_start = time.perf_counter()
        volume = data.compute()
        compute_time = time.perf_counter() - compute_start
        np.save(volume_path, volume)

        # Step 3: Enrich metadata
        stub.update(summarize_data(data))
        stub.update({
            "sha256": hashlib.sha256(volume.tobytes()).hexdigest(),
            "file_size_bytes": volume.nbytes,
            "status": "complete"
        })
        save_metadata_atomically(metadata_path, stub)

        return f"✅ {name} saved in {compute_time:.2f}s"

    except Exception as e:
        traceback.print_exc()
        return f"❌ Failed to save {name}: {e}"


def main() -> None:
    print("🚀 Starting Zarr ingestion pipeline\n")

    try:
        arrays = load_zarr_arrays_from_s3(S3_URI, KNOWN_ZARR_PATH)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        print(f"💾 Launching parallel save and compute with {MAX_WORKERS} workers...\n")
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = {
                executor.submit(
                    save_volume_and_metadata, name, data, OUTPUT_DIR, S3_URI, KNOWN_ZARR_PATH, timestamp
                ): name
                for name, data in arrays.items()
            }

            for future in tqdm(as_completed(futures), total=len(futures), desc="🧪 Processing arrays"):
                name = futures[future]
                try:
                    result = future.result()
                    tqdm.write(result)
                except Exception as e:
                    tqdm.write(f"❌ Error processing '{name}': {e}")
                    traceback.print_exc()

        print("\n✅ Ingestion complete.")

    except Exception as e:
        print(f"\n🔥 Fatal error: {e}")
        traceback.print_exc()


if __name__ == "__main__":
    main()
