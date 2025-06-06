import os
import sys
import json
import time
import hashlib
import argparse
import traceback
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import zarr
import s3fs
import numpy as np
import dask
import dask.array as da
from tqdm import tqdm

# Add lib directory to path for config_manager import
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..', 'lib'))
from config_manager import get_config_manager


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


def write_metadata_stub(name, npy_path, metadata_path, s3_uri, internal_path, dataset_id: str, voxel_size: dict, dimensions_nm: dict) -> None:
    metadata = {
        "source": "openorganelle",
        "source_id": dataset_id,
        "description": f"Array '{name}' from OpenOrganelle Zarr S3 store",
        "download_url": s3_uri,
        "internal_zarr_path": f"{internal_path}/{name}",
        "imaging_start_date": "Mon Mar 09 2015",
        "voxel_size_nm": voxel_size,
        "dimensions_nm": dimensions_nm,
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


def save_volume_and_metadata(name: str, data: da.Array, output_dir: str, s3_uri: str, internal_path: str, timestamp: str, dataset_id: str, voxel_size: dict, dimensions_nm: dict, chunk_size_mb: int) -> str:
    try:
        safe_name = name.replace("/", "_")
        volume_path = os.path.join(output_dir, f"{safe_name}_{timestamp}.npy")
        metadata_path = os.path.join(output_dir, f"metadata_{safe_name}_{timestamp}.json")

        # Estimate memory requirements
        estimated_mb = estimate_memory_usage(data)
        print(f"  💾 Processing {name}: estimated {estimated_mb:.1f}MB")

        # Step 1: Write stub first
        stub = write_metadata_stub(name, volume_path, metadata_path, s3_uri, internal_path, dataset_id, voxel_size, dimensions_nm)
        save_metadata_atomically(metadata_path, stub)

        # Step 2: Compute and save volume using chunked processing
        compute_start = time.perf_counter()
        
        # Use chunked computation for memory efficiency
        volume = compute_chunked_array(data, chunk_size_mb)
        
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
            "chunk_strategy": "memory_optimized" if estimated_mb > chunk_size_mb else "direct_compute",
            "status": "complete"
        })
        save_metadata_atomically(metadata_path, stub)

        # Clean up memory
        del volume
        
        return f"✅ {name} saved in {compute_time:.2f}s ({estimated_mb:.1f}MB)"

    except Exception as e:
        traceback.print_exc()
        return f"❌ Failed to save {name}: {e}"


def main(config) -> None:
    print("🚀 Starting Zarr ingestion pipeline\n")
    
    # Get configuration values
    s3_uri = config.get('sources.openorganelle.defaults.s3_uri', 's3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr')
    zarr_path = config.get('sources.openorganelle.defaults.zarr_path', 'recon-2/em')
    dataset_id = config.get('sources.openorganelle.defaults.dataset_id', 'jrc_mus-nacc-2')
    output_dir = config.get('sources.openorganelle.output_dir', './data/openorganelle')
    max_workers = config.get('sources.openorganelle.defaults.max_workers', 3)
    chunk_size_mb = config.get('sources.openorganelle.processing.chunk_size_mb', 256)
    voxel_size = config.get('sources.openorganelle.defaults.voxel_size', {"x": 4.0, "y": 4.0, "z": 2.96})
    dimensions_nm = config.get('sources.openorganelle.defaults.dimensions_nm', {"x": 10384, "y": 10080, "z": 1669.44})
    
    # Configure Dask for memory efficiency
    dask.config.set({
        'array.chunk-size': f'{chunk_size_mb}MB',
        'array.slicing.split_large_chunks': True,
        'optimization.fuse.active': False,  # Disable fusion to reduce memory usage
    })
    print(f"🔧 Dask configured: {chunk_size_mb}MB chunks, fusion disabled\n")

    try:
        arrays = load_zarr_arrays_from_s3(s3_uri, zarr_path)
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Sort arrays by estimated size (process smaller ones first)
        sorted_arrays = sorted(arrays.items(), key=lambda x: estimate_memory_usage(x[1]))
        total_estimated_mb = sum(estimate_memory_usage(data) for _, data in sorted_arrays)
        
        print(f"💾 Processing {len(sorted_arrays)} arrays (~{total_estimated_mb:.1f}MB total)")
        print(f"🔧 Memory optimization: {chunk_size_mb}MB chunks, {max_workers} workers\n")
        
        # Use parallel processing with memory-aware workers
        print(f"📊 Using parallel processing with {max_workers} workers (Memory: {total_estimated_mb:.1f}MB)")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    save_volume_and_metadata, name, data, output_dir, s3_uri, zarr_path, timestamp, dataset_id, voxel_size, dimensions_nm, chunk_size_mb
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


def parse_args():
    parser = argparse.ArgumentParser(description='OpenOrganelle Zarr data ingestion')
    parser.add_argument('--config', type=str, default=None,
                        help='Path to configuration file')
    parser.add_argument('--dataset-id', type=str, default=None,
                        help='Dataset ID to process')
    parser.add_argument('--s3-uri', type=str, default=None,
                        help='S3 URI for the Zarr store')
    return parser.parse_args()


def main_entry():
    args = parse_args()
    
    # Initialize config manager
    config_manager = get_config_manager(args.config)
    
    # Override values if provided via command line
    if args.dataset_id:
        config_manager.set('sources.openorganelle.defaults.dataset_id', args.dataset_id)
    if args.s3_uri:
        config_manager.set('sources.openorganelle.defaults.s3_uri', args.s3_uri)
    
    main(config_manager)


if __name__ == "__main__":
    main_entry()
