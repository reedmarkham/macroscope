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
import logging

# Add lib directory to path for config_manager import
sys.path.append('/app/lib')
from config_manager import get_config_manager

# Configure logging
logger = logging.getLogger(__name__)

# TODO: Import helper functions when refactoring is complete
# from helpers import (
#     load_zarr_arrays_from_s3,
#     summarize_data,
#     estimate_memory_usage,
#     write_metadata_stub,
#     save_metadata_atomically,
#     get_memory_info,
#     get_cpu_info
# )
# from processing import (
#     compute_chunked_array,
#     save_volume_and_metadata_streaming
# )


def load_zarr_arrays_from_s3(bucket_uri: str, internal_path: str) -> dict:
    if not bucket_uri.startswith("s3://"):
        raise ValueError("URI must start with s3://")

    logger.info(" [STEP 1/7] Connecting to S3 bucket: %s", bucket_uri)
    start = time.perf_counter()

    logger.info("    Initializing S3 filesystem (anonymous access)...")
    s3 = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(root=bucket_uri, s3=s3, check=False)
    logger.info("    S3 connection established")

    logger.info("    Loading Zarr metadata...")
    try:
        zgroup = zarr.open_consolidated(store)
        logger.info("    Loaded Zarr using consolidated metadata (optimized)")
    except (zarr.errors.MetadataError, KeyError) as e:
        logger.info("   ⚠ Consolidated metadata not found, falling back to open_group: %s", e)
        zgroup = zarr.open_group(store)
        logger.info("    Loaded Zarr using standard metadata")

    logger.info("    Accessing internal path: %s", internal_path)
    try:
        subgroup = zgroup[internal_path]
        logger.info("    Successfully accessed subgroup: %s", internal_path)
    except KeyError:
        raise ValueError(f"❌ Subgroup '{internal_path}' not found in Zarr store")

    def load_subgroup_arrays(subname):
        logger.info("       Scanning subgroup: %s", subname)
        result = {}
        subsubgroup = subgroup[subname]
        array_keys = list(subsubgroup.array_keys())
        logger.info("       Found %s arrays in {subname}", len(array_keys))
        
        for arr_key in array_keys:
            full_key = f"{subname}/{arr_key}"
            try:
                zarr_array = subsubgroup[arr_key]
                dask_array = da.from_zarr(zarr_array)
                result[full_key] = dask_array
                logger.info("          %s: {dask_array.shape} {dask_array.dtype}", full_key)
            except Exception as e:
                logger.info("          Failed to load %s: {e}", full_key)
        
        return result

    arrays = {}
    group_keys = list(subgroup.group_keys())
    logger.debug(" [STEP 2/7] Found %s subgroup(s); starting metadata loading", len(group_keys))
    logger.info("    Subgroups to process: %s", group_keys)

    # Use single worker for emergency mode to prevent memory issues
    max_workers = int(os.environ.get('MAX_WORKERS', '1'))  # Default to 1 for emergency mode
    logger.info("    Using %s worker(s) for metadata loading", max_workers)
    
    if max_workers == 1:
        # Sequential processing for emergency mode
        for i, name in enumerate(group_keys):
            logger.info("    Processing subgroup %s/{len(group_keys)}: {name}", i+1)
            try:
                result = load_subgroup_arrays(name)
                arrays.update(result)
                logger.info("    Completed %s: {len(result)} arrays loaded", name)
            except Exception as e:
                logger.info("    Error loading subgroup '%s': {e}", name)
                traceback.print_exc()
    else:
        # Parallel processing for normal mode
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(load_subgroup_arrays, name): name for name in group_keys}
            for future in tqdm(as_completed(futures), total=len(futures), desc="📥 Loading arrays"):
                name = futures[future]
                try:
                    result = future.result()
                    arrays.update(result)
                    logger.info("    Completed %s: {len(result)} arrays loaded", name)
                except Exception as e:
                    logger.info("    Error loading subgroup '%s': {e}", name)
                    traceback.print_exc()

    if not arrays:
        raise ValueError(f"❌ No arrays found under '{internal_path}'")

    elapsed = time.perf_counter() - start
    logger.info(" [STEP 2/7]  Array discovery complete: %s arrays in {elapsed:.2f}s", len(arrays))
    
    # Log array summary
    logger.info("    Array inventory:")
    for name, array in arrays.items():
        size_mb = estimate_memory_usage(array)
        logger.info("      • %s: {array.shape} {array.dtype} ({size_mb:.1f}MB)", name)
    
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
    logger.info("   Array: %sMB, target chunk: {chunk_size_mb}MB, current RSS: {mem_info['rss_mb']:.1f}MB", total_size_mb:.1f)
    
    # Emergency memory-aware threshold based on array size for 2GB container
    if total_size_mb <= 4:  # Very small arrays - compute directly
        logger.info("   Computing entire array (%sMB) - very small", total_size_mb:.1f)
        return data.compute()
    elif total_size_mb <= chunk_size_mb:  # Small arrays - minimal chunking
        logger.info("   Computing with minimal chunking (%sMB) - small", total_size_mb:.1f)
        # Force even smaller chunks for memory safety
        emergency_chunks = [min(chunk, 32) for chunk in data.chunksize]
        data = data.rechunk(emergency_chunks)
        return data.compute()
    
    # Large arrays - conservative chunking strategy to prevent timeouts
    logger.info("   Using conservative chunking strategy (%sMB)", total_size_mb:.1f)
    
    # Calculate conservative chunk sizes to prevent SIGTERM at 89% completion
    optimal_chunks = []
    for i, (dim_size, current_chunk) in enumerate(zip(data.shape, data.chunksize)):
        if total_size_mb > 1000:  # Very large arrays (>1GB) - conservative approach
            if dim_size > 512:
                # Use larger chunks to reduce overhead and prevent timeouts
                target_chunk_size = min(current_chunk, max(64, dim_size // 6))  # Fewer, larger chunks
            elif dim_size > 128:
                target_chunk_size = min(current_chunk, max(32, dim_size // 4))   # Conservative chunks
            else:
                target_chunk_size = min(current_chunk, dim_size)
        elif total_size_mb > 200:  # Medium arrays (200MB-1GB) - conservative approach
            if dim_size > 256:
                target_chunk_size = min(current_chunk, max(64, dim_size // 8))  # Conservative chunks
            elif dim_size > 64:
                target_chunk_size = min(current_chunk, max(32, dim_size // 4))   # Conservative chunks
            else:
                target_chunk_size = min(current_chunk, dim_size)
        else:  # Smaller arrays - moderate chunking
            if dim_size > 128:
                target_chunk_size = min(current_chunk, max(32, dim_size // 8))  # Conservative chunks
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
        
        # Emergency memory management: Avoid creating too many chunks (2GB container limit)
        if expected_chunks > 1000:  # Much lower threshold for 2GB container
            logger.info("   Emergency: Too many chunks (%s), using minimal chunking for 2GB container", expected_chunks)
            # Use very conservative chunks to fit in 2GB memory
            optimal_chunks = [min(current, max(16, dim // 8)) for dim, current in zip(data.shape, data.chunksize)]
            expected_chunks = np.prod([(dim + chunk - 1) // chunk for dim, chunk in zip(data.shape, optimal_chunks)])
            logger.info("     🛡 Reduced to %s chunks for memory safety", expected_chunks)
        
        logger.info("   Rechunking from %s to {optimal_chunks}", data.chunksize)
        logger.info("     Creating %s chunks for optimized processing", expected_chunks)
        data = data.rechunk(optimal_chunks)
    
    # Compute with progress monitoring for large arrays
    chunk_counts = [len(chunks) for chunks in data.chunks]
    total_chunk_count = chunk_counts[0] * chunk_counts[1] * chunk_counts[2]
    logger.info("   Computing with %s x {chunk_counts[1]} x {chunk_counts[2]} = {total_chunk_count} chunks...", chunk_counts[0])
    
    try:
        computation_start = time.perf_counter()
        
        # Get max_workers setting early for logging
        max_workers = int(os.environ.get('MAX_WORKERS', 4))
        
        # Show detailed progress for arrays based on new emergency thresholds
        if total_size_mb > 100:  # Large array threshold for 2GB container
            logger.info("  ⏱ Large array detected (%sMB), processing with emergency settings...", total_size_mb:.1f)
            logger.info("      Array shape: %s, dtype: {data.dtype}", data.shape)
            logger.info("      Processing %s chunks sequentially (emergency mode)", total_chunk_count)
            logger.info("     🛡 Memory-safe processing with %sMB chunks", chunk_size_mb)
            logger.info("   Starting computation at %s...", time.strftime('%H:%M:%S'))
        elif total_size_mb > 25:  # Medium array threshold
            logger.info("   Medium array (%sMB), processing with conservative settings...", total_size_mb:.1f)
            logger.info("      Shape: %s, chunks: {total_chunk_count}", data.shape)
        else:
            logger.info("   Small array (%sMB), quick processing...", total_size_mb:.1f)
            
        # Use Dask's compute with optimized parallel processing
        cpu_info_start = get_cpu_info()
        
        # Optimize for CPU utilization while maintaining memory safety
        cpu_count = os.cpu_count() or 4
        compute_workers = min(cpu_count, max_workers, 4)  # Balance performance and memory
        
        with dask.config.set({
            'scheduler': 'threads' if compute_workers > 1 else 'synchronous',
            'num_workers': compute_workers,
            'threaded.num_workers': compute_workers,
            'array.optimize_graph': True,   # Enable graph optimization for performance
            'array.slicing.split_large_chunks': True,  # Aggressive chunk splitting
            'optimization.fuse.active': True,  # Enable fusion for better performance
        }):
            logger.info("      Computing with %s workers for better CPU utilization", compute_workers)
            # Monitor memory before computation
            pre_compute_mem = get_memory_info()
            logger.info("      Pre-compute memory: %sMB", pre_compute_mem['rss_mb']:.1f)
            
            result = data.compute()
        
        computation_time = time.perf_counter() - computation_start
        final_mem = get_memory_info()
        cpu_info_end = get_cpu_info()
        
        # Show timing and performance info based on array size  
        rate_mbps = total_size_mb / computation_time if computation_time > 0 else 0
        avg_cpu = (cpu_info_start['process_cpu_percent'] + cpu_info_end['process_cpu_percent']) / 2
        
        if total_size_mb > 100:  # Large arrays - detailed feedback
            logger.info("   Large array computation complete in %ss ({rate_mbps:.1f} MB/s)", computation_time:.1f)
            logger.info("      Final memory: %sMB RSS, avg CPU: {avg_cpu:.1f}%", final_mem['rss_mb']:.1f)
            logger.info("     🛡 Emergency mode successful - no SIGKILL")
            
            # Memory utilization feedback for emergency mode
            memory_usage_pct = final_mem['rss_mb'] / (2048)  # 2GB container limit
            if memory_usage_pct > 0.8:
                logger.info("     ⚠ High memory usage (%s%) - consider reducing chunk size further", memory_usage_pct*100:.1f)
            else:
                logger.info("      Good memory usage (%s%) - emergency settings working", memory_usage_pct*100:.1f)
                
        elif total_size_mb > 25:  # Medium arrays - moderate feedback
            logger.info("   Medium array computation complete in %ss ({rate_mbps:.1f} MB/s)", computation_time:.1f)
            logger.info("      Memory: %sMB RSS", final_mem['rss_mb']:.1f)
        else:  # Small arrays - minimal feedback
            logger.info("   Small array computation complete in %ss", computation_time:.1f)
            
        return result
        
    except MemoryError as e:
        logger.info("   Memory error during computation: %s", e)
        logger.info("   Try reducing ZARR_CHUNK_SIZE_MB or MAX_WORKERS environment variables")
        raise
    except Exception as e:
        logger.info("   Computation failed: %s", e)
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
                logger.info("    ⚠ Very small memory estimate: %sMB for shape {data.shape}, dtype {data.dtype}", estimated_mb:.6f)
            
            return estimated_mb
        
        # Method 3: Final fallback with better logging
        total_elements = np.prod(data.shape) if hasattr(data, 'shape') else 1000000  # 1M default
        estimated_bytes = total_elements * 4  # Assume 4 bytes per element
        estimated_mb = estimated_bytes / (1024 * 1024)
        logger.info("    ⚠ Using fallback memory estimate: %sMB", estimated_mb:.1f)
        return estimated_mb
        
    except Exception as e:
        logger.info("     Memory estimation failed: %s", e)
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


def save_volume_and_metadata_streaming(name: str, data: da.Array, output_dir: str, s3_uri: str, internal_path: str, timestamp: str, dataset_id: str, voxel_size: dict, dimensions_nm: dict, chunk_size_mb: int) -> str:
    """
    Stream very large arrays to disk chunk-by-chunk to avoid memory issues.
    Uses disk spilling and progressive processing for arrays >500MB.
    """
    try:
        safe_name = name.replace("/", "_")
        volume_path = os.path.join(output_dir, f"{safe_name}_{timestamp}.zarr")  # Save as Zarr instead of NPY
        metadata_path = os.path.join(output_dir, f"metadata_{safe_name}_{timestamp}.json")

        # Estimate memory requirements
        estimated_mb = estimate_memory_usage(data)
        mem_info = get_memory_info()
        logger.info("   Streaming large array %s: estimated {estimated_mb:.1f}MB (current memory: {mem_info['rss_mb']:.1f}MB)", name)

        # Progress bar for overall streaming process
        with tqdm(total=5, desc=f"🌊 Streaming {safe_name}", 
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                 position=0, leave=True) as pbar:
            
            # Step 1: Write stub first
            pbar.set_description(f"🌊 {safe_name}: Writing metadata stub")
            stub = write_metadata_stub(name, volume_path, metadata_path, s3_uri, internal_path, dataset_id, voxel_size, dimensions_nm)
            save_metadata_atomically(metadata_path, stub)
            pbar.update(1)

            # Step 2: Process array chunk-by-chunk with disk spilling
            pbar.set_description(f"🌊 {safe_name}: Calculating streaming chunks")
            logger.info("   Streaming processing: chunk-by-chunk to avoid memory limits")
            
            # Optimize chunk size for streaming - balance memory and performance
            streaming_chunk_size = min(chunk_size_mb, 8)  # Increase to 8MB for better throughput
            optimal_chunks = []
            
            # Calculate optimal chunks based on CPU count and memory
            cpu_count = os.cpu_count() or 4
            target_chunks_per_worker = 4  # 4 chunks per worker for good parallelism
            
            for dim_size in data.shape:
                # Calculate chunk size that balances memory and parallelism
                elements_per_chunk = (streaming_chunk_size * 1024 * 1024) // data.dtype.itemsize
                base_chunk_size = max(32, int(elements_per_chunk ** (1/len(data.shape))))
                
                # Ensure we have enough chunks for parallel processing
                min_chunks_needed = cpu_count * target_chunks_per_worker
                if dim_size // base_chunk_size < min_chunks_needed:
                    # Make chunks smaller to enable more parallelism
                    chunk_size = max(16, dim_size // min_chunks_needed)
                else:
                    chunk_size = min(dim_size, base_chunk_size)
                
                optimal_chunks.append(chunk_size)
            
            logger.info("      Rechunking to optimized streaming chunks: %s", optimal_chunks)
            logger.info("      Expected parallelism: ~%s chunks across {cpu_count} CPUs", np.prod([(d + c - 1) // c for d, c in zip(data.shape, optimal_chunks)]))
            
            # Show rechunking progress
            with tqdm(total=1, desc="     🔄 Rechunking array", 
                     bar_format="{desc}: {percentage:3.0f}%|{bar}| [{elapsed}]",
                     position=1, leave=False) as rebar:
                streaming_data = data.rechunk(optimal_chunks)
                rebar.update(1)
            
            pbar.update(1)
            
            # Step 3: Save directly to Zarr format (supports streaming)
            pbar.set_description(f"🌊 {safe_name}: Streaming to Zarr format")
            logger.info("   Streaming to Zarr format: %s", volume_path)
            
            # Use Zarr with compression to save space
            import zarr
            zarr_store = zarr.DirectoryStore(volume_path)
            logger.info("      Created Zarr store: %s", volume_path)
            
            # Calculate approximate total chunks for progress tracking
            total_elements = np.prod(data.shape)
            chunk_elements = np.prod(optimal_chunks)
            estimated_chunks = int(total_elements / chunk_elements)
            logger.info("      Calculated streaming: %s elements in {estimated_chunks:,} chunks", total_elements:,)
            logger.info("      Chunk details: %s = {chunk_elements:,} elements per chunk", optimal_chunks)
            
            # Configure Dask for streaming with optimized parallel processing
            # Use CPU count for better utilization while maintaining memory safety
            cpu_count = os.cpu_count() or 4
            optimal_workers = min(cpu_count, 4)  # Cap at 4 workers for memory safety
            
            logger.info("      Using %s workers for streaming (detected {cpu_count} CPUs)", optimal_workers)
            
            with dask.config.set({
                'scheduler': 'threads',  # Use threaded scheduler for CPU utilization
                'num_workers': optimal_workers,
                'array.chunk-size': f'{streaming_chunk_size}MB',
                'distributed.worker.memory.target': 0.4,  # Higher target for streaming
                'distributed.worker.memory.spill': 0.5,   # Conservative spill
                'threaded.num_workers': optimal_workers,  # Explicitly set thread count
                'array.slicing.split_large_chunks': True,  # Enable chunk splitting
                'optimization.fuse.active': True,  # Enable fusion for performance
            }):
                start_time = time.perf_counter()
                
                # Stream to Zarr with optimized parallel processing and progress tracking
                logger.info("      Streaming %s chunks to Zarr with {optimal_workers} workers...", estimated_chunks)
                
                # Use zarr with explicit compression and chunk optimization
                import zarr
                import numcodecs
                
                # Create zarr array with optimized settings for streaming
                zarr_array = zarr.open_array(
                    zarr_store,
                    mode='w',
                    shape=streaming_data.shape,
                    dtype=streaming_data.dtype,
                    chunks=optimal_chunks,
                    compressor=numcodecs.Blosc(cname='lz4', clevel=1, shuffle=numcodecs.Blosc.BITSHUFFLE),  # Fast compression
                )
                
                # Stream to Zarr with progress tracking using dask compute with progress
                logger.info("      Computing and writing chunks with %s workers...", optimal_workers)
                
                # Use dask compute with progress monitoring
                try:
                    # Use da.store with compute=False to get the computation graph
                    store_result = da.store(streaming_data, zarr_array, lock=False, compute=False)
                    
                    # Progress tracking with tqdm
                    with tqdm(total=estimated_chunks, desc="     💿 Writing Zarr chunks", 
                             unit="chunks", bar_format="{desc}: {n_fmt}/{total_fmt} chunks |{bar}| [{elapsed}<{remaining}, {rate_fmt}]",
                             position=1, leave=False) as chunk_pbar:
                        
                        # Compute with the current dask configuration
                        da.compute(store_result)
                        
                        # Update progress (this will show completion)
                        chunk_pbar.update(estimated_chunks)
                        
                except Exception as store_error:
                    logger.info("     ⚠  Store operation failed: %s", store_error)
                    logger.info("      Falling back to direct Zarr assignment...")
                    
                    # Fallback: Direct assignment with progress tracking
                    with tqdm(total=estimated_chunks, desc="     💿 Writing Zarr chunks (fallback)", 
                             unit="chunks", bar_format="{desc}: {n_fmt}/{total_fmt} chunks |{bar}| [{elapsed}<{remaining}, {rate_fmt}]",
                             position=1, leave=False) as chunk_pbar:
                        
                        # Direct assignment (this should work with any zarr version)
                        zarr_array[:] = streaming_data.compute()
                        chunk_pbar.update(estimated_chunks)
                
                stream_time = time.perf_counter() - start_time
            
            pbar.update(1)
            logger.info("   Streaming complete in %ss", stream_time:.1f)

            # Step 4: Calculate summary stats from streamed data
            pbar.set_description(f"🌊 {safe_name}: Computing statistics")
            logger.info("   Computing summary statistics from streamed data...")
            
            # Read back from Zarr for stats (memory-efficient)
            zarr_array = zarr.open(zarr_store)
            dask_from_zarr = da.from_zarr(zarr_array)
            
            # Compute stats with progress tracking
            with tqdm(total=1, desc="     📊 Computing mean value", 
                     bar_format="{desc}: {percentage:3.0f}%|{bar}| [{elapsed}]",
                     position=1, leave=False) as stats_pbar:
                mean_val = dask_from_zarr.mean().compute()
                stats_pbar.update(1)
            
            pbar.update(1)
            
            # Step 5: Enrich metadata
            pbar.set_description(f"🌊 {safe_name}: Finalizing metadata")
            stub.update({
                "volume_shape": data.shape,
                "dtype": str(data.dtype),
                "chunk_size": optimal_chunks,
                "global_mean": float(mean_val),
                "file_format": "zarr",
                "compression": "default",
                "streaming_processed": True,
                "processing_time_seconds": round(stream_time, 2),
                "chunk_strategy": "streaming_disk_spill",
                "estimated_chunks": estimated_chunks,
                "status": "complete"
            })
            save_metadata_atomically(metadata_path, stub)
            pbar.update(1)

            # Clean up memory
            del streaming_data
            import gc
            gc.collect()
        
        return f"Streamed {name} in {stream_time:.1f}s ({estimated_mb:.1f}MB → Zarr format)"

    except Exception as e:
        traceback.print_exc()
        return f"❌ Failed to stream {name}: {e}"


def save_volume_and_metadata_downsampled(name: str, data: da.Array, output_dir: str, s3_uri: str, internal_path: str, timestamp: str, dataset_id: str, voxel_size: dict, dimensions_nm: dict, chunk_size_mb: int, downsample_factor: int = 4) -> str:
    """
    Process very large arrays by downsampling them to fit in memory.
    Creates multiple resolution levels like an image pyramid.
    """
    try:
        safe_name = name.replace("/", "_")
        metadata_path = os.path.join(output_dir, f"metadata_{safe_name}_{timestamp}.json")

        # Estimate memory requirements
        estimated_mb = estimate_memory_usage(data)
        logger.info("  📉 Downsampling large array %s: {estimated_mb:.1f}MB → targeting <{chunk_size_mb*4}MB", name)

        # Progress bar for overall downsampling process
        with tqdm(total=4, desc=f"🔽 Downsampling {safe_name}", 
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}]",
                 position=0, leave=True) as pbar:

            # Step 1: Write stub first
            pbar.set_description(f"🔽 {safe_name}: Writing metadata stub")
            stub = write_metadata_stub(name, f"{safe_name}_pyramid", metadata_path, s3_uri, internal_path, dataset_id, voxel_size, dimensions_nm)
            save_metadata_atomically(metadata_path, stub)
            pbar.update(1)

            # Step 2: Create multiple resolution levels
            pbar.set_description(f"🔽 {safe_name}: Creating pyramid levels")
            logger.info("   Creating resolution pyramid with factor %s", downsample_factor)
            
            pyramid_results = {}
            current_data = data
            level = 0
            max_levels = 10  # Safety limit
            
            # Calculate how many levels we'll need
            temp_size = estimated_mb
            estimated_levels = 0
            while temp_size > chunk_size_mb * 4 and estimated_levels < max_levels:
                estimated_levels += 1
                temp_size /= (downsample_factor ** len(data.shape))  # Reduction in all dimensions
            
            with tqdm(total=estimated_levels, desc="     🔽 Creating pyramid levels", 
                     unit="levels", position=1, leave=False) as level_pbar:
                
                while estimate_memory_usage(current_data) > chunk_size_mb * 4:  # Process until manageable size
                    level += 1
                    if level > max_levels:
                        logger.info("     ⚠ Reached maximum levels (%s), stopping", max_levels)
                        break
                        
                    level_pbar.set_description(f"     🔽 Level {level}: {current_data.shape}")
                    
                    # Downsample by factor (using every Nth element)
                    slices = tuple(slice(None, None, downsample_factor) for _ in current_data.shape)
                    current_data = current_data[slices]
                    level_pbar.update(1)
                    
                    # If this level is now manageable, process it
                    if estimate_memory_usage(current_data) <= chunk_size_mb * 4:
                        logger.info("      Level %s is manageable: {current_data.shape} ({estimate_memory_usage(current_data):.1f}MB)", level)
                        break

            pbar.update(1)
            
            # Step 3: Process the final downsampled level
            pbar.set_description(f"🔽 {safe_name}: Processing level {level}")
            level_path = os.path.join(output_dir, f"{safe_name}_level{level}_{timestamp}.npy")
            
            with tqdm(total=1, desc=f"     🔄 Computing level {level}", 
                     bar_format="{desc}: {percentage:3.0f}%|{bar}| [{elapsed}]",
                     position=1, leave=False) as compute_pbar:
                start_time = time.perf_counter()
                volume = compute_chunked_array(current_data, chunk_size_mb)
                compute_time = time.perf_counter() - start_time
                compute_pbar.update(1)
            
            # Save downsampled volume with progress
            with tqdm(total=1, desc=f"     💿 Saving level {level}", 
                     bar_format="{desc}: {percentage:3.0f}%|{bar}| [{elapsed}]",
                     position=1, leave=False) as save_pbar:
                np.save(level_path, volume)
                save_pbar.update(1)
            
            # Calculate adjusted voxel size
            adjusted_voxel_size = {k: v * (downsample_factor ** level) for k, v in voxel_size.items()}
            
            pyramid_results[f"level_{level}"] = {
                "path": level_path,
                "shape": volume.shape,
                "voxel_size_nm": adjusted_voxel_size,
                "downsample_factor": downsample_factor ** level,
                "processing_time_seconds": round(compute_time, 2),
                "global_mean": float(volume.mean())
            }
            
            del volume
            import gc
            gc.collect()
            pbar.update(1)
        
            # Step 4: Enrich metadata with pyramid info
            pbar.set_description(f"🔽 {safe_name}: Finalizing metadata")
            stub.update({
                "volume_shape": data.shape,
                "dtype": str(data.dtype),
                "processing_method": "progressive_downsampling",
                "pyramid_levels": pyramid_results,
                "original_size_mb": estimated_mb,
                "final_level": level,
                "total_reduction_factor": downsample_factor ** level,
                "status": "complete"
            })
            save_metadata_atomically(metadata_path, stub)
            pbar.update(1)
        
        return f"Downsampled {name} to level {level} ({downsample_factor**level}x reduction)"

    except Exception as e:
        traceback.print_exc()
        return f"❌ Failed to downsample {name}: {e}"


def save_volume_and_metadata(name: str, data: da.Array, output_dir: str, s3_uri: str, internal_path: str, timestamp: str, dataset_id: str, voxel_size: dict, dimensions_nm: dict, chunk_size_mb: int) -> str:
    try:
        safe_name = name.replace("/", "_")
        volume_path = os.path.join(output_dir, f"{safe_name}_{timestamp}.npy")
        metadata_path = os.path.join(output_dir, f"metadata_{safe_name}_{timestamp}.json")

        # Estimate memory requirements and show current usage
        estimated_mb = estimate_memory_usage(data)
        mem_info = get_memory_info()
        logger.info("   Processing %s: estimated {estimated_mb:.1f}MB (current memory: {mem_info['rss_mb']:.1f}MB)", name)

        # Step 1: Write stub first
        stub = write_metadata_stub(name, volume_path, metadata_path, s3_uri, internal_path, dataset_id, voxel_size, dimensions_nm)
        save_metadata_atomically(metadata_path, stub)

        # Step 2: Compute and save volume using adaptive chunked processing
        compute_start = time.perf_counter()
        
        # Show progress info for larger arrays
        if estimated_mb > 100:
            logger.info("   Large array processing: %s ({estimated_mb:.1f}MB)", name)
            logger.info("      Started at: %s", time.strftime('%H:%M:%S'))
        
        # Use adaptive chunked computation
        volume = compute_chunked_array(data, chunk_size_mb)
        
        compute_time = time.perf_counter() - compute_start
        
        # Show completion info for larger arrays
        if estimated_mb > 100:
            rate_mbps = estimated_mb / compute_time if compute_time > 0 else 0
            logger.info("   Completed at: %s ({compute_time:.1f}s, {rate_mbps:.1f} MB/s)", time.strftime('%H:%M:%S'))
        
        # Save volume with memory-efficient approach
        logger.info("   Saving volume to disk (%sMB)", volume.nbytes / (1024*1024):.1f)
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

        # Clean up memory aggressively
        del volume
        import gc
        gc.collect()  # Force garbage collection to free memory immediately
        
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


def get_cpu_info():
    """Get current CPU usage information."""
    try:
        import psutil
        # Get overall system CPU usage
        cpu_percent = psutil.cpu_percent(interval=0.1)
        # Get process-specific CPU usage
        process = psutil.Process()
        process_cpu = process.cpu_percent()
        # Get CPU count
        cpu_count = psutil.cpu_count()
        return {
            'system_cpu_percent': cpu_percent,
            'process_cpu_percent': process_cpu,
            'cpu_count': cpu_count,
            'cpu_utilization': min(100.0, process_cpu)  # Cap at 100%
        }
    except ImportError:
        return {'system_cpu_percent': 0, 'process_cpu_percent': 0, 'cpu_count': 4, 'cpu_utilization': 0}


def main(config) -> None:
    logger.info(" [MAIN] Starting OpenOrganelle Zarr Ingestion Pipeline")
    logger.info("=" * 80)
    logger.info(" Emergency Memory Management Mode: Ultra-Conservative Settings")
    logger.info("=" * 80)
    pipeline_start = time.perf_counter()
    
    # Show current time
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    logger.info(" Pipeline started at: %s\n", current_time)
    
    # Get configuration values with environment variable overrides
    s3_uri = config.get('sources.openorganelle.defaults.s3_uri', 's3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr')
    zarr_path = config.get('sources.openorganelle.defaults.zarr_path', 'recon-2/em')
    dataset_id = config.get('sources.openorganelle.defaults.dataset_id', 'jrc_mus-nacc-2')
    output_dir = os.environ.get('EM_DATA_DIR', config.get('sources.openorganelle.output_dir', './data/openorganelle'))
    
    # Get memory settings from environment variables with emergency minimal approach to prevent SIGKILL
    max_workers = int(os.environ.get('MAX_WORKERS', config.get('sources.openorganelle.processing.max_workers', 1)))  # Single worker to prevent OOM
    chunk_size_mb = int(os.environ.get('ZARR_CHUNK_SIZE_MB', config.get('sources.openorganelle.processing.chunk_size_mb', 8)))  # Minimal chunks
    memory_limit_gb = int(os.environ.get('MEMORY_LIMIT_GB', 2))
    max_array_size_mb = int(os.environ.get('MAX_ARRAY_SIZE_MB', config.get('sources.openorganelle.processing.max_array_size_mb', 500)))
    
    # Large array processing configuration
    large_array_mode = os.environ.get('LARGE_ARRAY_MODE', config.get('sources.openorganelle.processing.large_array_mode', 'skip'))
    downsample_factor = int(config.get('sources.openorganelle.processing.downsample_factor', 4))
    streaming_chunk_mb = int(config.get('sources.openorganelle.processing.streaming_chunk_mb', 2))
    
    voxel_size = config.get('sources.openorganelle.defaults.voxel_size', {"x": 4.0, "y": 4.0, "z": 2.96})
    dimensions_nm = config.get('sources.openorganelle.defaults.dimensions_nm', {"x": 10384, "y": 10080, "z": 1669.44})
    
    # Show initial memory usage
    mem_info = get_memory_info()
    logger.info(" Initial memory usage: %sMB RSS", mem_info['rss_mb']:.1f)
    logger.info(" Emergency minimal settings: %sGB limit, {max_workers} workers, {chunk_size_mb}MB chunks", memory_limit_gb)
    logger.warning("  Large array filtering: Arrays > %sMB will be skipped to prevent SIGKILL", max_array_size_mb)
    
    # Configure Dask for balanced performance and memory management
    # Detect available CPU cores for optimal utilization
    cpu_count = os.cpu_count() or 4
    optimal_workers = min(max_workers, cpu_count, 4)  # Balance workers with memory limits
    
    logger.info(" CPU optimization: Using %s workers (detected {cpu_count} CPUs, max allowed: {max_workers})", optimal_workers)
    
    dask.config.set({
        'array.chunk-size': f'{chunk_size_mb}MB',        # Configurable chunk size
        'array.slicing.split_large_chunks': True,        # Split large chunks aggressively
        'optimization.fuse.active': True,                # Enable fusion for better performance
        'optimization.cull.active': True,                # Enable task culling to free memory
        'array.rechunk.threshold': 4,                    # Moderate rechunking threshold
        
        # Balanced memory management for performance and safety
        'distributed.worker.memory.target': 0.4,        # 40% memory usage target
        'distributed.worker.memory.spill': 0.5,         # Spill at 50%
        'distributed.worker.memory.pause': 0.6,         # Pause at 60%
        'distributed.worker.memory.terminate': 0.7,     # Terminate at 70%
        
        # Optimized threading for CPU utilization
        'threaded.num_workers': optimal_workers,        # Use detected optimal workers
        'scheduler': 'threads' if optimal_workers > 1 else 'synchronous',  # Adaptive scheduler
        
        # Performance optimization settings
        'array.chunk-opts.tempdirs': ['/tmp'],          # Use temp storage for spilling
        'temporary-directory': '/tmp',                  # Temp directory for large operations
        'distributed.worker.memory.recent-to-old-time': '30s',  # Longer memory management cycles
        'distributed.worker.memory.rebalance.measure': 'managed',  # Monitor managed memory
        
        # Additional performance optimizations
        'array.optimize_graph': True,                   # Enable graph optimization
        'dataframe.optimize_graph': True,              # Enable dataframe optimization  
        'delayed.optimize_graph': True,                # Enable delayed optimization
    })
    logger.info(" Dask configured: %sMB chunks, balanced performance and memory management", chunk_size_mb)
    logger.info("   🛡 Memory limits: 40% target, 50% spill, 60% pause, 70% terminate")
    logger.info("    Multi-threaded processing with %s workers for better CPU utilization\n", optimal_workers)

    try:
        # STEP 1-2: Array discovery (handled in load_zarr_arrays_from_s3)
        arrays = load_zarr_arrays_from_s3(s3_uri, zarr_path)
        
        logger.info("\n📁 [STEP 3/7] Setting up output directory: %s", output_dir)
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info("    Output directory ready, timestamp: %s", timestamp)

        logger.info("\n [STEP 4/7] Analyzing arrays and filtering by size")
        logger.info("    Calculating memory requirements for %s arrays...", len(arrays))
        
        # Calculate sizes with progress
        array_sizes = []
        for i, (name, data) in enumerate(arrays.items()):
            size = estimate_memory_usage(data)
            array_sizes.append((name, data, size))
            logger.info("      %s/{len(arrays)} {name}: {size:.1f}MB", i+1:2d)
        
        logger.info("    Memory analysis complete")
        
        # Apply large array processing strategy
        logger.info("   🛡 Applying large array strategy: %s (max {max_array_size_mb}MB per array)", large_array_mode)
        
        if large_array_mode == "skip":
            processable_arrays = [(name, data, size) for name, data, size in array_sizes if size <= max_array_size_mb]
            skipped_arrays = [(name, data, size) for name, data, size in array_sizes if size > max_array_size_mb]
            large_arrays_special = []
        else:
            # All arrays are processable, but large ones get special treatment
            processable_arrays = [(name, data, size) for name, data, size in array_sizes if size <= max_array_size_mb]
            large_arrays_special = [(name, data, size) for name, data, size in array_sizes if size > max_array_size_mb]
            skipped_arrays = []
        
        total_estimated_mb = sum(size for _, _, size in processable_arrays)
        skipped_mb = sum(size for _, _, size in skipped_arrays)
        
        if skipped_arrays:
            logger.info("   ⚠  SKIPPING %s large arrays ({skipped_mb:.1f}MB total) to prevent SIGKILL:", len(skipped_arrays))
            for name, _, size in skipped_arrays:
                logger.info("      🚫 %s ({size:.1f}MB) - exceeds {max_array_size_mb}MB limit", name)
        elif large_arrays_special:
            special_mb = sum(size for _, _, size in large_arrays_special)
            logger.info("    SPECIAL PROCESSING for %s large arrays ({special_mb:.1f}MB total):", len(large_arrays_special))
            logger.info("       Mode: %s", large_array_mode)
            for name, _, size in large_arrays_special:
                logger.info("       %s ({size:.1f}MB) - will use {large_array_mode} processing", name)
        else:
            logger.info("    All arrays are within normal processing limits")
        
        # Sort by size but process strategically
        logger.info("    Sorting %s processable arrays by size...", len(processable_arrays))
        sorted_arrays = sorted(processable_arrays, key=lambda x: x[2])  # Sort by size
        
        # Categorize arrays for processing strategy (using smaller thresholds)
        logger.info("    Categorizing arrays by size:")
        small_arrays = [(name, data) for name, data, size in sorted_arrays if size < 25]    # < 25MB
        medium_arrays = [(name, data) for name, data, size in sorted_arrays if 25 <= size < 100]  # 25MB-100MB  
        large_arrays = [(name, data) for name, data, size in sorted_arrays if size >= 100]   # >= 100MB
        
        logger.info("       Small (<25MB): %s arrays", len(small_arrays))
        logger.info("       Medium (25-100MB): %s arrays", len(medium_arrays)) 
        logger.info("       Large (≥100MB): %s arrays", len(large_arrays))
        
        logger.info(" Found %s processable arrays, total estimated size: {total_estimated_mb:.1f}MB", len(sorted_arrays))
        logger.info("    Categories: %s small (<25MB), {len(medium_arrays)} medium (25-100MB), {len(large_arrays)} large (≥100MB)", len(small_arrays))
        if skipped_arrays:
            logger.info("   🚫 Skipped: %s arrays ({skipped_mb:.1f}MB) - too large for {memory_limit_gb}GB memory limit", len(skipped_arrays))
        
        # Show array size breakdown with categories
        logger.info("\n Processable array size breakdown:")
        for i, (name, data, size_mb) in enumerate(sorted_arrays):
            category = "🟢" if size_mb < 25 else "🟡" if size_mb < 100 else "🔴"
            logger.info("  %s. {category} {name:<20} {size_mb:>8.1f}MB", i+1:2d)
        
        if skipped_arrays:
            logger.info("\n🚫 Skipped arrays (>%sMB):", max_array_size_mb)
            for i, (name, _, size_mb) in enumerate(skipped_arrays):
                logger.info("  %s.  {name:<20} {size_mb:>8.1f}MB (too large)", i+1:2d)
        
        logger.info("\n Emergency minimal processing strategy: %s worker, {chunk_size_mb}MB chunks", max_workers)
        logger.info("   🛡 Strategy: Sequential processing only to prevent SIGKILL")
        logger.info("   📉 Arrays >%sMB are skipped to stay within {memory_limit_gb}GB memory limit", max_array_size_mb)
        
        # Processing strategy based on array mix
        if large_arrays:
            logger.info("    Large array optimization: Will process %s large arrays with adaptive chunking", len(large_arrays))
        if len(small_arrays) > 3:
            logger.info("    Small array batch processing: Will process %s small arrays in parallel", len(small_arrays))
        
        logger.info("\n [STEP 5/7] Starting array processing pipeline")
        logger.info("    Processing strategy: Emergency sequential mode")
        logger.info("   🛡 Memory budget: %sGB container limit", memory_limit_gb)
        logger.info("   ⚙  Settings: %s worker, {chunk_size_mb}MB chunks, synchronous Dask", max_workers)
        
        # Emergency sequential processing strategy to prevent SIGKILL  
        all_arrays_to_process = sorted_arrays  # Normal arrays
        
        # Add large arrays for special processing if enabled
        if large_arrays_special:
            logger.info("    Will process %s large arrays using {large_array_mode} mode", len(large_arrays_special))
            all_arrays_to_process.extend(large_arrays_special)
        
        # Process arrays one at a time to minimize memory usage
        completed_count = 0
        total_arrays = len(all_arrays_to_process)
        successful_count = 0
        failed_count = 0
        start_time = time.perf_counter()
        
        logger.info("\n [STEP 6/7] Processing %s arrays sequentially", total_arrays)
        logger.info("=" * 80)
        
        # Main processing progress bar
        with tqdm(total=total_arrays, desc="🔄 Processing arrays", 
                 bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]",
                 unit="arrays", position=0, leave=True) as main_pbar:
            
            for name, data, size_mb in all_arrays_to_process:
                logger.info("\n Processing array %s/{total_arrays}: {name} ({size_mb:.1f}MB)", completed_count + 1)
                
                # Update main progress bar description
                main_pbar.set_description(f"🔄 Processing {name} ({size_mb:.1f}MB)")
                main_pbar.set_postfix({
                    'current': name.split('/')[-1],
                    'size_mb': f'{size_mb:.1f}',
                    'success': successful_count,
                    'failed': failed_count
                })
                
                # Emergency memory monitoring before processing
                mem_info = get_memory_info()
                memory_threshold_mb = memory_limit_gb * 1024 * 0.5  # 50% threshold - very conservative
                
                if mem_info['rss_mb'] > memory_threshold_mb:
                    logger.info("   EMERGENCY: Memory usage (%sMB) exceeds 50% of {memory_limit_gb}GB limit", mem_info['rss_mb']:.0f)
                    logger.info("      Skipping array %s ({size_mb:.1f}MB) to prevent SIGKILL", name)
                    completed_count += 1
                    main_pbar.update(1)
                    continue
                
                if mem_info['rss_mb'] > (memory_limit_gb * 1024 * 0.3):  # 30% threshold
                    logger.info("  ⚠ Warning: Memory usage (%sMB), forcing aggressive cleanup", mem_info['rss_mb']:.0f)
                    import gc
                    gc.collect()
                    mem_info = get_memory_info()
                    logger.info("   Memory after cleanup: %sMB", mem_info['rss_mb']:.0f)
                    
                    # Double-check after cleanup
                    if mem_info['rss_mb'] > memory_threshold_mb:
                        logger.info("   Still too high after cleanup, skipping %s", name)
                        completed_count += 1
                        main_pbar.update(1)
                        continue
                
                # Calculate estimated time remaining
                if completed_count > 0:
                    elapsed = time.perf_counter() - start_time
                    avg_time = elapsed / completed_count
                    remaining_arrays = total_arrays - completed_count
                    eta_seconds = remaining_arrays * avg_time
                    eta_str = f"{eta_seconds/60:.1f}min" if eta_seconds > 60 else f"{eta_seconds:.0f}s"
                    logger.info("   ⏱  Progress: %s/{total_arrays} | Avg: {avg_time:.1f}s/array | ETA: {eta_str}", completed_count)
                
                try:
                    array_start = time.perf_counter()
                    
                    # Choose processing method based on array size and mode
                    if size_mb > max_array_size_mb:
                        logger.info("    Large array detected: using %s processing", large_array_mode)
                        if large_array_mode == "stream":
                            result = save_volume_and_metadata_streaming(
                                name, data, output_dir, s3_uri, zarr_path, 
                                timestamp, dataset_id, voxel_size, dimensions_nm, streaming_chunk_mb
                            )
                        elif large_array_mode == "downsample":
                            result = save_volume_and_metadata_downsampled(
                                name, data, output_dir, s3_uri, zarr_path, 
                                timestamp, dataset_id, voxel_size, dimensions_nm, chunk_size_mb, downsample_factor
                            )
                        else:
                            result = f"Skipped {name} - large array mode disabled"
                    else:
                        # Normal processing for regular-sized arrays
                        result = save_volume_and_metadata(
                            name, data, output_dir, s3_uri, zarr_path, 
                            timestamp, dataset_id, voxel_size, dimensions_nm, chunk_size_mb
                        )
                    
                    array_time = time.perf_counter() - array_start
                    completed_count += 1
                    successful_count += 1
                    
                    category = "🔴" if size_mb >= 100 else "🟡" if size_mb >= 25 else "🟢"
                    logger.info(" [%s/{total_arrays}] {category} {name} completed in {array_time:.1f}s", completed_count)
                    logger.info("    Result: %s", result)
                    
                    # Update progress bar with success
                    main_pbar.update(1)
                    main_pbar.set_postfix({
                        'success': successful_count,
                        'failed': failed_count,
                        'rate': f'{array_time:.1f}s'
                    })
                    
                    # Show progress summary every 5 arrays
                    if completed_count % 5 == 0 or completed_count == total_arrays:
                        elapsed = time.perf_counter() - start_time
                        rate = completed_count / elapsed if elapsed > 0 else 0
                        mem_info = get_memory_info()
                        logger.info("    Progress Summary: %s/{total_arrays} arrays | {rate:.2f} arrays/min | Memory: {mem_info['rss_mb']:.0f}MB", completed_count)
                    
                    # Force cleanup after each array to prevent memory accumulation
                    import gc
                    gc.collect()
                
                except Exception as e:
                    array_time = time.perf_counter() - array_start if 'array_start' in locals() else 0
                    completed_count += 1
                    failed_count += 1
                    
                    category = "🔴" if size_mb >= 100 else "🟡" if size_mb >= 25 else "🟢"
                    logger.error(" [%s/{total_arrays}] {category} FAILED '{name}' ({size_mb:.1f}MB) after {array_time:.1f}s", completed_count)
                    logger.info("    Error: %s", e)
                    traceback.print_exc()
                    
                    # Update progress bar with failure
                    main_pbar.update(1)
                    main_pbar.set_postfix({
                        'success': successful_count,
                        'failed': failed_count,
                        'rate': f'{array_time:.1f}s'
                    })
                    
                    # Force cleanup even on errors
                    import gc
                    gc.collect()
        

        # Final performance summary
        processing_time = time.perf_counter() - start_time
        pipeline_time = time.perf_counter() - pipeline_start
        total_data_gb = total_estimated_mb / 1024
        throughput = total_data_gb / (processing_time / 3600) if processing_time > 0 else 0  # GB/hour
        
        logger.info("\n" + "=" * 80)
        logger.info(" [STEP 7/7] PIPELINE COMPLETION SUMMARY")
        logger.info("=" * 80)
        
        current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(" Pipeline completed at: %s", current_time)
        
        logger.info("\n PROCESSING RESULTS:")
        logger.info("    Successful: %s/{total_arrays} arrays", successful_count)
        logger.info("    Failed: %s/{total_arrays} arrays", failed_count)
        if skipped_arrays:
            logger.info("   🚫 Skipped (too large): %s arrays ({skipped_mb:.1f}MB)", len(skipped_arrays))
        logger.info("    Success rate: %s%", (successful_count/total_arrays*100):.1f)
        
        logger.info("\n⏱  TIMING BREAKDOWN:")
        logger.info("    Array processing time: %ss ({processing_time/60:.1f}min)", processing_time:.1f)
        logger.info("    Total pipeline time: %ss ({pipeline_time/60:.1f}min)", pipeline_time:.1f)
        logger.info("    Average per array: %ss", processing_time/total_arrays:.1f)
        if successful_count > 0:
            logger.info("    Average per success: %ss", processing_time/successful_count:.1f)
        
        logger.info("\n DATA SUMMARY:")
        logger.info("    Data processed: %s GB", total_data_gb:.2f)
        logger.info("    Throughput: %s GB/hour", throughput:.1f)
        logger.info("    Processing rate: %s arrays/min", successful_count/(processing_time/60):.1f)
        
        final_mem = get_memory_info()
        logger.info("\n🛡  MEMORY MANAGEMENT:")
        logger.info("    Final memory usage: %sMB", final_mem['rss_mb']:.1f)
        logger.info("    Container limit: %sMB", memory_limit_gb*1024:.0f)
        logger.info("    Peak utilization: %s%", (final_mem['rss_mb']/(memory_limit_gb*1024)*100):.1f)
        logger.info("    No SIGKILL events: Emergency settings successful")

    except Exception as e:
        logger.info("\n Fatal error: %s", e)
        logger.info(" Performance tips:")
        logger.info("   - Reduce ZARR_CHUNK_SIZE_MB (currently %sMB)", chunk_size_mb)
        logger.info("   - Reduce MAX_WORKERS (currently %s)", max_workers)
        logger.info("   - Increase MEMORY_LIMIT_GB (currently %sGB)", memory_limit_gb)
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
