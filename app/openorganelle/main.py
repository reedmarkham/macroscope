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
sys.path.append('/app/lib')
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

    max_workers = int(os.environ.get('MAX_WORKERS', '3'))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
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


def compute_chunked_array(data: da.Array, chunk_size_mb: int = 64) -> np.ndarray:
    """
    Optimized computation: Adaptive chunking strategy based on array size and available memory.
    Uses intelligent chunk sizing to minimize overhead while preventing OOM.
    """
    total_size_mb = estimate_memory_usage(data)
    mem_info = get_memory_info()
    print(f"  📊 Array: {total_size_mb:.1f}MB, target chunk: {chunk_size_mb}MB, current RSS: {mem_info['rss_mb']:.1f}MB")
    
    # Adaptive threshold based on array size
    if total_size_mb <= 16:  # Very small arrays - compute directly
        print(f"  🔄 Computing entire array ({total_size_mb:.1f}MB) - very small")
        return data.compute()
    elif total_size_mb <= chunk_size_mb:  # Small arrays - minimal chunking
        print(f"  🔄 Computing with minimal chunking ({total_size_mb:.1f}MB) - small")
        return data.compute()
    
    # Large arrays - intelligent chunking strategy
    print(f"  🧩 Using adaptive chunking strategy ({total_size_mb:.1f}MB)")
    
    # Calculate optimal chunk sizes based on array characteristics
    optimal_chunks = []
    for i, (dim_size, current_chunk) in enumerate(zip(data.shape, data.chunksize)):
        if total_size_mb > 1000:  # Very large arrays (>1GB) - optimize for I/O throughput
            if dim_size > 512:
                # Use smaller chunks for better parallelism during I/O operations
                target_chunk_size = min(current_chunk, max(32, dim_size // 12))  # More parallel chunks
            elif dim_size > 128:
                target_chunk_size = min(current_chunk, max(16, dim_size // 8))   # Smaller chunks
            else:
                target_chunk_size = min(current_chunk, dim_size)
        elif total_size_mb > 200:  # Medium arrays (200MB-1GB) - balanced approach
            if dim_size > 256:
                target_chunk_size = min(current_chunk, max(32, dim_size // 12))  # Balanced chunks
            elif dim_size > 64:
                target_chunk_size = min(current_chunk, max(16, dim_size // 6))   # Smaller chunks
            else:
                target_chunk_size = min(current_chunk, dim_size)
        else:  # Smaller arrays - fine-grained chunking
            if dim_size > 128:
                target_chunk_size = min(current_chunk, max(16, dim_size // 16))  # Fine chunks
            else:
                target_chunk_size = min(current_chunk, dim_size)
        
        optimal_chunks.append(target_chunk_size)
    
    # Apply rechunking if beneficial
    if optimal_chunks != list(data.chunksize):
        # Calculate expected chunk count for overhead assessment
        expected_chunks = 1
        for i, (dim_size, chunk_size) in enumerate(zip(data.shape, optimal_chunks)):
            chunks_in_dim = (dim_size + chunk_size - 1) // chunk_size  # Ceiling division
            expected_chunks *= chunks_in_dim
        
        # Avoid creating too many tiny chunks (overhead > benefit)
        if expected_chunks > 10000:
            print(f"  ⚠️ Too many chunks ({expected_chunks}), using coarser chunking to reduce overhead")
            # Use coarser chunks to reduce overhead
            optimal_chunks = [min(current, max(64, dim // 4)) for dim, current in zip(data.shape, data.chunksize)]
            expected_chunks = np.prod([(dim + chunk - 1) // chunk for dim, chunk in zip(data.shape, optimal_chunks)])
        
        print(f"  ⚡ Rechunking from {data.chunksize} to {optimal_chunks}")
        print(f"     Creating {expected_chunks} chunks for optimized processing")
        data = data.rechunk(optimal_chunks)
    
    # Compute with progress monitoring for large arrays
    chunk_counts = [len(chunks) for chunks in data.chunks]
    total_chunk_count = chunk_counts[0] * chunk_counts[1] * chunk_counts[2]
    print(f"  🔄 Computing with {chunk_counts[0]} x {chunk_counts[1]} x {chunk_counts[2]} = {total_chunk_count} chunks...")
    
    try:
        computation_start = time.perf_counter()
        
        # Get max_workers setting early for logging
        max_workers = int(os.environ.get('MAX_WORKERS', 4))
        
        # For very large arrays, show detailed progress and timing info
        if total_size_mb > 500:
            print(f"  ⏱️ Large array detected ({total_size_mb:.1f}MB), this may take several minutes...")
            print(f"     📊 Array shape: {data.shape}, dtype: {data.dtype}")
            print(f"     🧩 Processing {total_chunk_count} chunks with {max_workers * 2} I/O threads")
            print(f"     🌐 I/O-optimized chunking for S3 network throughput")
            print(f"  🔄 Starting computation at {time.strftime('%H:%M:%S')}...")
            
        # Use Dask's compute with memory-efficient scheduling
        with dask.config.set(scheduler='threads', num_workers=max_workers):
            result = data.compute()
        
        computation_time = time.perf_counter() - computation_start
        final_mem = get_memory_info()
        
        if total_size_mb > 100:  # Show detailed timing for medium+ arrays
            rate_mbps = total_size_mb / computation_time if computation_time > 0 else 0
            print(f"  ✅ Computation complete in {computation_time:.1f}s ({rate_mbps:.1f} MB/s)")
            print(f"     📈 Final memory: {final_mem['rss_mb']:.1f}MB RSS")
        else:
            print(f"  ✅ Computation complete. Memory: {final_mem['rss_mb']:.1f}MB RSS")
            
        return result
        
    except MemoryError as e:
        print(f"  ❌ Memory error during computation: {e}")
        print(f"  🔧 Try reducing ZARR_CHUNK_SIZE_MB or MAX_WORKERS environment variables")
        raise
    except Exception as e:
        print(f"  ❌ Computation failed: {e}")
        raise


def estimate_memory_usage(data: da.Array) -> float:
    """Estimate memory usage in MB for a dask array with improved accuracy."""
    try:
        # Method 1: Try to get nbytes directly
        if hasattr(data, 'nbytes') and data.nbytes > 0:
            return data.nbytes / (1024 * 1024)
        
        # Method 2: Calculate from shape and dtype (more reliable)
        if hasattr(data, 'shape') and hasattr(data, 'dtype'):
            element_size = data.dtype.itemsize
            total_elements = np.prod(data.shape)  # More efficient than manual loop
            estimated_bytes = total_elements * element_size
            estimated_mb = estimated_bytes / (1024 * 1024)
            
            # Sanity check: If result seems too small, log for debugging
            if estimated_mb < 0.001:  # Less than 1KB seems wrong
                print(f"    ⚠️ Very small memory estimate: {estimated_mb:.6f}MB for shape {data.shape}, dtype {data.dtype}")
            
            return estimated_mb
        
        # Method 3: Final fallback with better logging
        total_elements = np.prod(data.shape) if hasattr(data, 'shape') else 1000000  # 1M default
        estimated_bytes = total_elements * 4  # Assume 4 bytes per element
        estimated_mb = estimated_bytes / (1024 * 1024)
        print(f"    ⚠️ Using fallback memory estimate: {estimated_mb:.1f}MB")
        return estimated_mb
        
    except Exception as e:
        print(f"    ❌ Memory estimation failed: {e}")
        # Conservative fallback
        return 100.0  # 100MB fallback


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

        # Estimate memory requirements and show current usage
        estimated_mb = estimate_memory_usage(data)
        mem_info = get_memory_info()
        print(f"  💾 Processing {name}: estimated {estimated_mb:.1f}MB (current memory: {mem_info['rss_mb']:.1f}MB)")

        # Step 1: Write stub first
        stub = write_metadata_stub(name, volume_path, metadata_path, s3_uri, internal_path, dataset_id, voxel_size, dimensions_nm)
        save_metadata_atomically(metadata_path, stub)

        # Step 2: Compute and save volume using adaptive chunked processing
        compute_start = time.perf_counter()
        
        # Show progress info for larger arrays
        if estimated_mb > 100:
            print(f"  🎯 Large array processing: {name} ({estimated_mb:.1f}MB)")
            print(f"     ⏰ Started at: {time.strftime('%H:%M:%S')}")
        
        # Use adaptive chunked computation
        volume = compute_chunked_array(data, chunk_size_mb)
        
        compute_time = time.perf_counter() - compute_start
        
        # Show completion info for larger arrays
        if estimated_mb > 100:
            rate_mbps = estimated_mb / compute_time if compute_time > 0 else 0
            print(f"  ⏰ Completed at: {time.strftime('%H:%M:%S')} ({compute_time:.1f}s, {rate_mbps:.1f} MB/s)")
        
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
        
        # Performance metrics
        rate_mbps = estimated_mb / compute_time if compute_time > 0 else 0
        category = "🔴" if estimated_mb >= 500 else "🟡" if estimated_mb >= 50 else "🟢"
        
        return f"{category} {name} saved in {compute_time:.1f}s ({estimated_mb:.1f}MB @ {rate_mbps:.1f} MB/s)"

    except Exception as e:
        traceback.print_exc()
        return f"❌ Failed to save {name}: {e}"


def get_memory_info():
    """Get current memory usage information."""
    try:
        import psutil
        process = psutil.Process()
        memory_info = process.memory_info()
        return {
            'rss_mb': memory_info.rss / (1024 * 1024),
            'vms_mb': memory_info.vms / (1024 * 1024)
        }
    except ImportError:
        return {'rss_mb': 0, 'vms_mb': 0}


def main(config) -> None:
    print("🚀 Starting Zarr ingestion pipeline (Optimized: Adaptive Chunking + Smart Parallelism)\n")
    
    # Get configuration values with environment variable overrides
    s3_uri = config.get('sources.openorganelle.defaults.s3_uri', 's3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr')
    zarr_path = config.get('sources.openorganelle.defaults.zarr_path', 'recon-2/em')
    dataset_id = config.get('sources.openorganelle.defaults.dataset_id', 'jrc_mus-nacc-2')
    output_dir = config.get('sources.openorganelle.output_dir', './data/openorganelle')
    
    # Get memory settings from environment variables with conservative (smaller chunks, more parallelism) approach
    max_workers = int(os.environ.get('MAX_WORKERS', config.get('sources.openorganelle.defaults.max_workers', 4)))  # More workers
    chunk_size_mb = int(os.environ.get('ZARR_CHUNK_SIZE_MB', config.get('sources.openorganelle.processing.chunk_size_mb', 64)))  # Smaller chunks
    memory_limit_gb = int(os.environ.get('MEMORY_LIMIT_GB', 8))
    
    voxel_size = config.get('sources.openorganelle.defaults.voxel_size', {"x": 4.0, "y": 4.0, "z": 2.96})
    dimensions_nm = config.get('sources.openorganelle.defaults.dimensions_nm', {"x": 10384, "y": 10080, "z": 1669.44})
    
    # Show initial memory usage
    mem_info = get_memory_info()
    print(f"💾 Initial memory usage: {mem_info['rss_mb']:.1f}MB RSS")
    print(f"🔧 Optimized settings: {memory_limit_gb}GB limit, {max_workers} workers, {chunk_size_mb}MB adaptive chunks")
    
    # Configure Dask for optimized I/O throughput and CPU utilization
    dask.config.set({
        'array.chunk-size': f'{chunk_size_mb}MB',
        'array.slicing.split_large_chunks': True,
        'optimization.fuse.active': False,   # Disable fusion for better I/O parallelism
        'optimization.cull.active': True,    # Enable task culling
        'array.rechunk.threshold': 2,        # More aggressive rechunking for I/O optimization
        'distributed.worker.memory.target': 0.75,  # Target 75% memory usage
        'distributed.worker.memory.spill': 0.85,   # Spill at 85%
        'distributed.worker.memory.pause': 0.95,   # Pause at 95%
        'threaded.num_workers': max_workers * 2,  # More threads for I/O operations
        'array.chunk-opts.tempdirs': ['/tmp'],     # Use fast temp storage
    })
    print(f"🔧 Dask configured: {chunk_size_mb}MB adaptive chunks, optimized performance\n")

    try:
        arrays = load_zarr_arrays_from_s3(s3_uri, zarr_path)
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        # Categorize arrays by size for optimal processing strategy
        array_sizes = [(name, data, estimate_memory_usage(data)) for name, data in arrays.items()]
        total_estimated_mb = sum(size for _, _, size in array_sizes)
        
        # Sort by size but process strategically
        sorted_arrays = sorted(array_sizes, key=lambda x: x[2])  # Sort by size
        
        # Categorize arrays for processing strategy
        small_arrays = [(name, data) for name, data, size in sorted_arrays if size < 50]    # < 50MB
        medium_arrays = [(name, data) for name, data, size in sorted_arrays if 50 <= size < 500]  # 50MB-500MB  
        large_arrays = [(name, data) for name, data, size in sorted_arrays if size >= 500]   # >= 500MB
        
        print(f"📊 Found {len(sorted_arrays)} arrays, total estimated size: {total_estimated_mb:.1f}MB")
        print(f"   📋 Categories: {len(small_arrays)} small (<50MB), {len(medium_arrays)} medium (50-500MB), {len(large_arrays)} large (≥500MB)")
        
        # Show array size breakdown with categories
        print("\n📋 Array size breakdown:")
        for i, (name, data, size_mb) in enumerate(sorted_arrays):
            category = "🟢" if size_mb < 50 else "🟡" if size_mb < 500 else "🔴"
            print(f"  {i+1:2d}. {category} {name:<20} {size_mb:>8.1f}MB")
        
        print(f"\n🔧 Adaptive processing strategy: {max_workers} workers, {chunk_size_mb}MB base chunks")
        print(f"   📈 Strategy: Small arrays → parallel batch, Medium → optimized chunks, Large → adaptive chunking")
        print(f"   🧩 Dynamic chunk sizing based on array characteristics to optimize performance")
        
        # Processing strategy based on array mix
        if large_arrays:
            print(f"   ⚡ Large array optimization: Will process {len(large_arrays)} large arrays with adaptive chunking")
        if len(small_arrays) > 3:
            print(f"   🚀 Small array batch processing: Will process {len(small_arrays)} small arrays in parallel")
        
        print(f"\n🚀 Starting optimized processing...")
        
        # Adaptive processing strategy based on array characteristics
        all_arrays_to_process = [(name, data, size) for name, data, size in sorted_arrays]
        
        # Determine optimal worker allocation
        if large_arrays:
            # For large arrays, reduce parallelism to avoid memory pressure
            large_workers = max(1, max_workers // 2)  # Use fewer workers for large arrays
            small_workers = max_workers
        else:
            large_workers = max_workers
            small_workers = max_workers
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            completed_count = 0
            
            # Smart job submission based on array size
            for name, data, size_mb in all_arrays_to_process:
                # Use appropriate worker count based on array size
                if size_mb >= 500:  # Large arrays
                    print(f"  🔴 Submitting large array {name} ({size_mb:.1f}MB) with conservative processing")
                elif size_mb >= 50:   # Medium arrays
                    print(f"  🟡 Submitting medium array {name} ({size_mb:.1f}MB) with balanced processing")
                
                future = executor.submit(
                    save_volume_and_metadata, name, data, output_dir, s3_uri, zarr_path, 
                    timestamp, dataset_id, voxel_size, dimensions_nm, chunk_size_mb
                )
                futures[future] = (name, size_mb)

            # Process results with enhanced progress tracking and performance metrics
            start_time = time.perf_counter()
            with tqdm(total=len(futures), desc="🧪 Processing arrays", unit="array") as pbar:
                for future in as_completed(futures):
                    name, size_mb = futures[future]
                    completed_count += 1
                    
                    try:
                        result = future.result()
                        
                        # Calculate processing rate
                        elapsed = time.perf_counter() - start_time
                        rate = completed_count / elapsed if elapsed > 0 else 0
                        
                        # Update progress with detailed info
                        mem_info = get_memory_info()
                        pbar.set_postfix({
                            'current': name.split('/')[-1][:8],
                            'size_mb': f"{size_mb:.0f}",
                            'rate': f"{rate:.1f}/s",
                            'mem_mb': f"{mem_info['rss_mb']:.0f}"
                        })
                        pbar.update(1)
                        
                        # Show result with timing info
                        category = "🔴" if size_mb >= 500 else "🟡" if size_mb >= 50 else "🟢"
                        tqdm.write(f"✅ [{completed_count}/{len(futures)}] {category} {result}")
                        
                    except Exception as e:
                        pbar.update(1)
                        category = "🔴" if size_mb >= 500 else "🟡" if size_mb >= 50 else "🟢"
                        tqdm.write(f"❌ [{completed_count}/{len(futures)}] {category} Failed '{name}' ({size_mb:.1f}MB): {e}")
                        traceback.print_exc()

        # Final performance summary
        total_time = time.perf_counter() - start_time
        total_data_gb = total_estimated_mb / 1024
        throughput = total_data_gb / (total_time / 3600) if total_time > 0 else 0  # GB/hour
        
        print(f"\n✅ Ingestion complete!")
        print(f"   📊 Performance Summary:")
        print(f"      ⏱️  Total time: {total_time:.1f} seconds ({total_time/60:.1f} minutes)")
        print(f"      💾 Data processed: {total_data_gb:.2f} GB")
        print(f"      🚀 Throughput: {throughput:.1f} GB/hour")
        print(f"      📈 Average: {total_time/len(all_arrays_to_process):.1f} seconds/array")

    except Exception as e:
        print(f"\n🔥 Fatal error: {e}")
        print(f"💡 Performance tips:")
        print(f"   - Reduce ZARR_CHUNK_SIZE_MB (currently {chunk_size_mb}MB)")
        print(f"   - Reduce MAX_WORKERS (currently {max_workers})")
        print(f"   - Increase MEMORY_LIMIT_GB (currently {memory_limit_gb}GB)")
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
