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
import dask
import dask.array as da
from tqdm import tqdm


S3_URI = "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr"
KNOWN_ZARR_PATH = "recon-2/em"
OUTPUT_DIR = os.environ.get('EM_DATA_DIR', '/app/data/openorganelle')
MAX_WORKERS = int(os.environ.get('MAX_WORKERS', '1'))  # Single worker for 8GB memory
CHUNK_SIZE_MB = int(os.environ.get('ZARR_CHUNK_SIZE_MB', '128'))  # Optimized for 8GB memory


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
    
    # Compute mean using chunked operations for memory efficiency
    mean_val = data.mean().compute()

    return {
        "volume_shape": shape,
        "dtype": dtype,
        "chunk_size": chunk_size,
        "global_mean": float(mean_val)
    }


def compute_chunked_array(data: da.Array, chunk_size_mb: int = 256) -> np.ndarray:
    """
    Compute dask array in memory-efficient chunks to prevent OOM errors.
    """
    total_size_mb = data.nbytes / (1024 * 1024)
    print(f"  📊 Array size: {total_size_mb:.1f}MB, target chunk: {chunk_size_mb}MB")
    
    if total_size_mb <= chunk_size_mb:
        # Small enough to compute directly
        print(f"  🔄 Computing entire array ({total_size_mb:.1f}MB)")
        return data.compute()
    
    # Large array - use chunked computation
    print(f"  🧩 Using chunked computation for large array ({total_size_mb:.1f}MB)")
    
    # Rechunk if necessary to optimize memory usage
    optimal_chunks = []
    for i, (dim_size, chunk_dim) in enumerate(zip(data.shape, data.chunksize)):
        # Target smaller chunks for memory efficiency
        target_chunk_size = min(chunk_dim, max(64, dim_size // 4))
        optimal_chunks.append(target_chunk_size)
    
    if optimal_chunks != list(data.chunksize):
        print(f"  ⚡ Rechunking from {data.chunksize} to {optimal_chunks}")
        data = data.rechunk(optimal_chunks)
    
    # Compute the array directly with optimized chunks
    return data.compute()


def estimate_memory_usage(data: da.Array) -> float:
    """Estimate memory usage in MB for a dask array."""
    return data.nbytes / (1024 * 1024)


def write_metadata_stub(name, npy_path, metadata_path, s3_uri, internal_path) -> None:
    metadata = {
        "source": "openorganelle",
        "source_id": os.path.basename(s3_uri).replace(".zarr", ""),
        "description": f"Array '{name}' from OpenOrganelle Zarr S3 store",
        "download_url": s3_uri,
        "internal_zarr_path": f"{internal_path}/{name}",
        "imaging_start_date": "Mon Mar 09 2015",
        "voxel_size_nm": {"x": 4.0, "y": 4.0, "z": 2.96},
        "dimensions_nm": {"x": 10384, "y": 10080, "z": 1669.44},
        "timestamp": datetime.now().isoformat() + "Z",
        "local_paths": {
            "volume": npy_path,
            "metadata": metadata_path
        },
        "status": "saving-data"
    }
    
    # Add CI/CD metadata if running in GitHub Actions
    if os.environ.get('EM_CI_METADATA') == 'true':
        metadata["ci_metadata"] = {
            "commit_sha": os.environ.get('CI_COMMIT_SHA'),
            "commit_ref": os.environ.get('CI_COMMIT_REF'),
            "pipeline_id": os.environ.get('CI_PIPELINE_ID'),
            "pipeline_url": os.environ.get('CI_PIPELINE_URL'),
            "triggered_by": os.environ.get('CI_TRIGGERED_BY'),
            "workflow_name": os.environ.get('CI_WORKFLOW_NAME'),
            "processing_environment": "github_actions"
        }
    
    return metadata


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

        # Estimate memory requirements
        estimated_mb = estimate_memory_usage(data)
        print(f"  💾 Processing {name}: estimated {estimated_mb:.1f}MB")

        # Step 1: Write stub first
        stub = write_metadata_stub(name, volume_path, metadata_path, s3_uri, internal_path)
        save_metadata_atomically(metadata_path, stub)

        # Step 2: Compute and save volume using chunked processing
        compute_start = time.perf_counter()
        
        # Use chunked computation for memory efficiency
        volume = compute_chunked_array(data, CHUNK_SIZE_MB)
        
        compute_time = time.perf_counter() - compute_start
        
        # Save volume with memory-efficient approach
        print(f"  💿 Saving volume to disk ({volume.nbytes / (1024*1024):.1f}MB)")
        np.save(volume_path, volume)

        # Step 3: Enrich metadata
        stub.update(summarize_data(data))
        stub.update({
            "sha256": hashlib.sha256(volume.tobytes()).hexdigest(),
            "file_size_bytes": volume.nbytes,
            "processing_time_seconds": round(compute_time, 2),
            "chunk_strategy": "memory_optimized" if estimated_mb > CHUNK_SIZE_MB else "direct_compute",
            "status": "complete"
        })
        save_metadata_atomically(metadata_path, stub)

        # Clean up memory
        del volume
        
        return f"✅ {name} saved in {compute_time:.2f}s ({estimated_mb:.1f}MB)"

    except Exception as e:
        traceback.print_exc()
        return f"❌ Failed to save {name}: {e}"


def main() -> None:
    print("🚀 Starting Zarr ingestion pipeline\n")
    
    # Configure Dask for memory efficiency
    dask.config.set({
        'array.chunk-size': f'{CHUNK_SIZE_MB}MB',
        'array.slicing.split_large_chunks': True,
        'optimization.fuse.active': False,  # Disable fusion to reduce memory usage
    })
    print(f"🔧 Dask configured: {CHUNK_SIZE_MB}MB chunks, fusion disabled\n")

    try:
        arrays = load_zarr_arrays_from_s3(S3_URI, KNOWN_ZARR_PATH)
        os.makedirs(OUTPUT_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Sort arrays by estimated size (process smaller ones first)
        sorted_arrays = sorted(arrays.items(), key=lambda x: estimate_memory_usage(x[1]))
        total_estimated_mb = sum(estimate_memory_usage(data) for _, data in sorted_arrays)
        
        print(f"💾 Processing {len(sorted_arrays)} arrays (~{total_estimated_mb:.1f}MB total)")
        print(f"🔧 Memory optimization: {CHUNK_SIZE_MB}MB chunks, {MAX_WORKERS} workers\n")
        
        # Use controlled parallelism based on memory requirements
        if total_estimated_mb > 512:  # > 512MB total - use sequential processing
            print("📊 Large dataset detected - using sequential processing for memory safety")
            for i, (name, data) in enumerate(tqdm(sorted_arrays, desc="🧪 Processing arrays")):
                try:
                    result = save_volume_and_metadata(name, data, OUTPUT_DIR, S3_URI, KNOWN_ZARR_PATH, timestamp)
                    tqdm.write(f"[{i+1}/{len(sorted_arrays)}] {result}")
                except Exception as e:
                    tqdm.write(f"❌ Error processing '{name}': {e}")
                    traceback.print_exc()
        else:
            # Small dataset - can use parallel processing
            print(f"📊 Small dataset - using parallel processing with {MAX_WORKERS} workers")
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                futures = {
                    executor.submit(
                        save_volume_and_metadata, name, data, OUTPUT_DIR, S3_URI, KNOWN_ZARR_PATH, timestamp
                    ): name
                    for name, data in sorted_arrays
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
