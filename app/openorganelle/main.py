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
            logger.info("    Processing subgroup %d/%d: %s", i+1, len(group_keys), name)
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
        logger.info("      • %s: %s %s (%.1fMB)", name, array.shape, array.dtype, size_mb)
    
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
    Performance-optimized computation with empirically-tuned thresholds.
    Uses adaptive chunking strategy based on performance analysis: 13.6MB→4s, 110MB→30s.
    """
    total_size_mb = estimate_memory_usage(data)
    mem_info = get_memory_info()
    logger.info("   Array: %.1fMB, target chunk: %dMB, current RSS: %.1fMB", total_size_mb, chunk_size_mb, mem_info['rss_mb'])
    
    # Performance-optimized thresholds based on empirical analysis
    if total_size_mb <= 4:  # Very small arrays - direct computation
        logger.info("   Computing entire array (%.1fMB) - very small", total_size_mb)
        return data.compute()
    elif total_size_mb < 25:  # Small arrays (< 25MB) - minimal chunking
        logger.info("   Computing with minimal chunking (%.1fMB) - small array", total_size_mb)
        # Use smaller chunks for better memory efficiency
        efficient_chunks = [min(chunk, 16) for chunk in data.chunksize]
        data = data.rechunk(efficient_chunks)
        return data.compute()
    elif total_size_mb < 100:  # Medium arrays (25-100MB) - optimized chunking
        logger.info("   Using optimized chunking for medium array (%.1fMB)", total_size_mb)
        # Aggressive chunking to prevent the 13.6MB→4s to 110MB→30s performance cliff
        chunk_size_mb = min(chunk_size_mb, 8)  # Force smaller chunks
    else:
        # Large arrays (≥100MB) - this should rarely be called due to early streaming
        logger.info("   Using conservative chunking strategy for large array (%.1fMB)", total_size_mb)
    
    # Performance-optimized chunk calculation based on empirical analysis
    optimal_chunks = []
    for i, (dim_size, current_chunk) in enumerate(zip(data.shape, data.chunksize)):
        if total_size_mb >= 100:  # Large arrays (≥100MB) - should use streaming, but handle edge cases
            if dim_size > 512:
                target_chunk_size = min(current_chunk, max(32, dim_size // 8))  # Smaller chunks to prevent cliff
            elif dim_size > 128:
                target_chunk_size = min(current_chunk, max(16, dim_size // 6))   # Aggressive chunking
            else:
                target_chunk_size = min(current_chunk, dim_size)
        elif total_size_mb >= 25:  # Medium arrays (25-100MB) - aggressive optimization
            if dim_size > 256:
                target_chunk_size = min(current_chunk, max(16, dim_size // 12))  # Very aggressive chunking
            elif dim_size > 64:
                target_chunk_size = min(current_chunk, max(8, dim_size // 8))    # Small chunks
            else:
                target_chunk_size = min(current_chunk, dim_size)
        else:  # Small arrays (<25MB) - minimal chunking overhead
            if dim_size > 64:
                target_chunk_size = min(current_chunk, max(16, dim_size // 4))  # Balanced approach
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
            logger.info("  Large array detected (%.1fMB), processing with emergency settings...", total_size_mb)
            logger.info("      Array shape: %s, dtype: {data.dtype}", data.shape)
            logger.info("      Processing %s chunks sequentially (emergency mode)", total_chunk_count)
            logger.info("     🛡 Memory-safe processing with %sMB chunks", chunk_size_mb)
            logger.info("   Starting computation at %s...", time.strftime('%H:%M:%S'))
        elif total_size_mb > 25:  # Medium array threshold
            logger.info("   Medium array (%.1fMB), processing with conservative settings...", total_size_mb)
            logger.info("      Shape: %s, chunks: {total_chunk_count}", data.shape)
        else:
            logger.info("   Small array (%.1fMB), quick processing...", total_size_mb)
            
        # Use Dask's compute with optimized parallel processing
        cpu_info_start = get_cpu_info()
        
        # High-performance computation with maximum CPU utilization
        cpu_count = os.cpu_count() or 4
        compute_workers = min(cpu_count, max_workers)  # Use all available workers
        
        with dask.config.set({
            'scheduler': 'threads',  # Always use threaded scheduler for parallelism
            'num_workers': compute_workers,
            'threaded.num_workers': compute_workers,
            'array.optimize_graph': True,               # Enable graph optimization
            'array.slicing.split_large_chunks': True,   # Aggressive chunk splitting
            'optimization.fuse.active': True,           # Enable fusion
            'optimization.fuse.max-width': 8,           # More aggressive fusion
            'optimization.fuse.max-height': 8,          # Deeper fusion
            'optimization.cull.active': True,           # Task culling
        }):
            logger.info("      Computing with %d workers for maximum CPU utilization", compute_workers)
            # Monitor memory before computation
            pre_compute_mem = get_memory_info()
            logger.info("      Pre-compute memory: %.1fMB", pre_compute_mem['rss_mb'])
            
            result = data.compute()
        
        computation_time = time.perf_counter() - computation_start
        final_mem = get_memory_info()
        cpu_info_end = get_cpu_info()
        
        # Show timing and performance info based on array size  
        rate_mbps = total_size_mb / computation_time if computation_time > 0 else 0
        avg_cpu = (cpu_info_start['process_cpu_percent'] + cpu_info_end['process_cpu_percent']) / 2
        
        if total_size_mb > 100:  # Large arrays - detailed feedback
            logger.info("   Large array computation complete in %.1fs (%.1f MB/s)", computation_time, rate_mbps)
            logger.info("      Final memory: %.1fMB RSS, avg CPU: %.1f%%", final_mem['rss_mb'], avg_cpu)
            logger.info("     🛡 Emergency mode successful - no SIGKILL")
            
            # Memory utilization feedback for emergency mode
            memory_usage_pct = final_mem['rss_mb'] / (2048)  # 2GB container limit
            if memory_usage_pct > 0.8:
                logger.info("     High memory usage (%.1f%%) - consider reducing chunk size further", memory_usage_pct*100)
            else:
                logger.info("      Good memory usage (%.1f%%) - emergency settings working", memory_usage_pct*100)
                
        elif total_size_mb > 25:  # Medium arrays - moderate feedback
            logger.info("   Medium array computation complete in %.1fs (%.1f MB/s)", computation_time, rate_mbps)
            logger.info("      Memory: %.1fMB RSS", final_mem['rss_mb'])
        else:  # Small arrays - minimal feedback
            logger.info("   Small array computation complete in %.1fs", computation_time)
            
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
                logger.info("    Very small memory estimate: %.6fMB for shape %s, dtype %s", estimated_mb, data.shape, data.dtype)
            
            return estimated_mb
        
        # Method 3: Final fallback with better logging
        total_elements = np.prod(data.shape) if hasattr(data, 'shape') else 1000000  # 1M default
        estimated_bytes = total_elements * 4  # Assume 4 bytes per element
        estimated_mb = estimated_bytes / (1024 * 1024)
        logger.info("    Using fallback memory estimate: %.1fMB", estimated_mb)
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
        logger.info("   Streaming large array %s: estimated %.1fMB (current memory: %.1fMB)", name, estimated_mb, mem_info['rss_mb'])

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
            logger.info("      Calculated streaming: %s elements in %s chunks", f"{total_elements:,}", f"{estimated_chunks:,}")
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
            logger.info("   Streaming complete in %.1fs", stream_time)

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
        logger.info("  Downsampling large array %s: %.1fMB -> targeting <%dMB", name, estimated_mb, chunk_size_mb*4)

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
                        logger.info("      Level %d is manageable: %s (%.1fMB)", level, current_data.shape, estimate_memory_usage(current_data))
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
        logger.info("   Processing %s: estimated %.1fMB (current memory: %.1fMB)", name, estimated_mb, mem_info['rss_mb'])

        # Step 1: Write stub first
        stub = write_metadata_stub(name, volume_path, metadata_path, s3_uri, internal_path, dataset_id, voxel_size, dimensions_nm)
        save_metadata_atomically(metadata_path, stub)

        # Step 2: Compute and save volume using adaptive chunked processing
        compute_start = time.perf_counter()
        
        # Show progress info for larger arrays
        if estimated_mb > 100:
            logger.info("   Large array processing: %s (%.1fMB)", name, estimated_mb)
            logger.info("      Started at: %s", time.strftime('%H:%M:%S'))
        
        # Use adaptive chunked computation
        volume = compute_chunked_array(data, chunk_size_mb)
        
        compute_time = time.perf_counter() - compute_start
        
        # Show completion info for larger arrays
        if estimated_mb > 100:
            rate_mbps = estimated_mb / compute_time if compute_time > 0 else 0
            logger.info("   Completed at: %s (%.1fs, %.1f MB/s)", time.strftime('%H:%M:%S'), compute_time, rate_mbps)
        
        # Save volume with memory-efficient approach
        logger.info("   Saving volume to disk (%.1fMB)", volume.nbytes / (1024*1024))
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
    
    # Auto-detect system resources for optimal performance
    cpu_count = os.cpu_count() or 4
    
    # Dynamic memory detection using psutil if available
    try:
        import psutil
        total_memory_mb = psutil.virtual_memory().total / (1024 * 1024)
        available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
        memory_limit_gb = int(total_memory_mb / 1024)
        logger.info("Auto-detected system: %.1fGB total, %.1fGB available, %d CPUs", 
                   total_memory_mb/1024, available_memory_mb/1024, cpu_count)
    except ImportError:
        # Fallback to environment variables
        memory_limit_gb = int(os.environ.get('MEMORY_LIMIT_GB', 8))  # Assume 8GB default
        available_memory_mb = memory_limit_gb * 1024 * 0.8  # 80% of total
        logger.info("Using configured memory: %dGB limit, %d CPUs", memory_limit_gb, cpu_count)
    
    # High-performance settings based on available resources
    optimal_workers = min(cpu_count, 6, max(2, cpu_count // 2))  # Use 2-6 workers based on CPU count
    max_workers = int(os.environ.get('MAX_WORKERS', config.get('sources.openorganelle.processing.max_workers', optimal_workers)))
    
    # Larger chunks for better performance with high memory availability
    optimal_chunk_mb = 32 if available_memory_mb > 4000 else 16  # 32MB chunks with >4GB memory
    chunk_size_mb = int(os.environ.get('ZARR_CHUNK_SIZE_MB', config.get('sources.openorganelle.processing.chunk_size_mb', optimal_chunk_mb)))
    
    # Aggressive thresholds with high memory availability
    SMALL_ARRAY_THRESHOLD = 50   # Increased from 25MB
    MEDIUM_ARRAY_THRESHOLD = 500 # Increased from 100MB - process larger arrays directly  
    STREAMING_THRESHOLD = 1000   # Increased from 100MB - use streaming only for very large arrays
    
    # High-capacity memory limits based on available resources
    dynamic_max_mb = min(available_memory_mb * 0.4, 2000)  # Use up to 40% of available memory, max 2GB per array
    max_array_size_mb = int(os.environ.get('MAX_ARRAY_SIZE_MB', dynamic_max_mb))
    
    # Large array processing configuration optimized for high-resource systems
    large_array_mode = os.environ.get('LARGE_ARRAY_MODE', config.get('sources.openorganelle.processing.large_array_mode', 'stream'))
    downsample_factor = int(config.get('sources.openorganelle.processing.downsample_factor', 4))
    
    # High-performance streaming with larger chunks to reduce overhead (18,477 chunks → ~500-1000 chunks)
    optimal_streaming_mb = 64 if available_memory_mb > 6000 else 32  # Much larger chunks with high memory
    streaming_chunk_mb = int(config.get('sources.openorganelle.processing.streaming_chunk_mb', optimal_streaming_mb))
    
    voxel_size = config.get('sources.openorganelle.defaults.voxel_size', {"x": 4.0, "y": 4.0, "z": 2.96})
    dimensions_nm = config.get('sources.openorganelle.defaults.dimensions_nm', {"x": 10384, "y": 10080, "z": 1669.44})
    
    # Show initial memory usage and high-performance configuration
    mem_info = get_memory_info()
    logger.info(" Initial memory usage: %.1fMB RSS", mem_info['rss_mb'])
    logger.info(" HIGH-PERFORMANCE CONFIGURATION:")
    logger.info("   System resources: %.1fGB available, %d CPUs detected", available_memory_mb/1024, cpu_count)
    logger.info("   Workers: %d (utilizing %d/%d CPUs)", max_workers, max_workers, cpu_count)
    logger.info("   Base chunks: %dMB, Streaming chunks: %dMB", chunk_size_mb, streaming_chunk_mb)
    logger.info(" Aggressive processing thresholds:")
    logger.info("   Small arrays (<%dMB): Direct processing", SMALL_ARRAY_THRESHOLD)
    logger.info("   Medium arrays (%d-%dMB): High-performance chunking", SMALL_ARRAY_THRESHOLD, MEDIUM_ARRAY_THRESHOLD) 
    logger.info("   Large arrays (>%dMB): Optimized streaming mode", STREAMING_THRESHOLD)
    logger.info(" Dynamic array limit: %dMB (%.1f%% of available memory)", max_array_size_mb, (max_array_size_mb/available_memory_mb)*100)
    
    # Configure Dask for high-performance multi-core processing
    logger.info(" Configuring Dask for %d workers across %d CPUs", max_workers, cpu_count)
    
    dask.config.set({
        # High-performance chunk settings
        'array.chunk-size': f'{chunk_size_mb}MB',        
        'array.slicing.split_large_chunks': True,        
        'optimization.fuse.active': True,                
        'optimization.cull.active': True,                
        'array.rechunk.threshold': 2,                    # More aggressive rechunking for performance
        
        # High-memory system configuration - much more aggressive settings
        'distributed.worker.memory.target': 0.6,        # 60% memory usage target (vs 40%)
        'distributed.worker.memory.spill': 0.75,        # Spill at 75% (vs 50%)
        'distributed.worker.memory.pause': 0.85,        # Pause at 85% (vs 60%)
        'distributed.worker.memory.terminate': 0.95,    # Terminate at 95% (vs 70%)
        
        # Multi-core threading optimization
        'threaded.num_workers': max_workers,            # Use all allocated workers
        'scheduler': 'threads',                         # Always use threaded scheduler for parallelism
        
        # High-performance optimization settings
        'array.chunk-opts.tempdirs': ['/tmp'],          
        'temporary-directory': '/tmp',                  
        'distributed.worker.memory.recent-to-old-time': '60s',  # Longer cycles for high memory
        'distributed.worker.memory.rebalance.measure': 'managed',  
        
        # Aggressive performance optimizations for high-resource systems
        'array.optimize_graph': True,                   
        'dataframe.optimize_graph': True,              
        'delayed.optimize_graph': True,
        'optimization.fuse.max-width': 8,               # More aggressive fusion
        'optimization.fuse.max-height': 8,              # Deeper fusion
        'array.slicing.split_large_chunks': True,       # Aggressive chunk splitting
    })
    logger.info(" Dask configured: %dMB chunks, high-performance multi-core processing", chunk_size_mb)
    logger.info("   🚀 Aggressive memory limits: 60% target, 75% spill, 85% pause, 95% terminate")
    logger.info("   ⚡ Multi-threaded processing with %d workers for maximum CPU utilization", max_workers)
    logger.info("   🎯 Optimized for %d CPUs with %.1fGB available memory\n", cpu_count, available_memory_mb/1024)

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
            logger.info("      %2d/%d %s: %.1fMB", i+1, len(arrays), name, size)
        
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
            logger.info("   SKIPPING %d large arrays (%.1fMB total) to prevent SIGKILL:", len(skipped_arrays), skipped_mb)
            for name, _, size in skipped_arrays:
                logger.info("      %s (%.1fMB) - exceeds %dMB limit", name, size, max_array_size_mb)
        elif large_arrays_special:
            special_mb = sum(size for _, _, size in large_arrays_special)
            logger.info("    SPECIAL PROCESSING for %d large arrays (%.1fMB total):", len(large_arrays_special), special_mb)
            logger.info("       Mode: %s", large_array_mode)
            for name, _, size in large_arrays_special:
                logger.info("       %s (%.1fMB) - will use %s processing", name, size, large_array_mode)
        else:
            logger.info("    All arrays are within normal processing limits")
        
        # Sort by size but process strategically
        logger.info("    Sorting %s processable arrays by size...", len(processable_arrays))
        sorted_arrays = sorted(processable_arrays, key=lambda x: x[2])  # Sort by size
        
        # Categorize arrays for processing strategy (using performance-optimized thresholds)
        logger.info("    Categorizing arrays by performance profile:")
        small_arrays = [(name, data) for name, data, size in sorted_arrays if size < SMALL_ARRAY_THRESHOLD]    
        medium_arrays = [(name, data) for name, data, size in sorted_arrays if SMALL_ARRAY_THRESHOLD <= size < MEDIUM_ARRAY_THRESHOLD]  
        large_arrays = [(name, data) for name, data, size in sorted_arrays if size >= MEDIUM_ARRAY_THRESHOLD]   
        
        logger.info("       Small (<%dMB): %d arrays - fast direct processing", SMALL_ARRAY_THRESHOLD, len(small_arrays))
        logger.info("       Medium (%d-%dMB): %d arrays - optimized chunking", SMALL_ARRAY_THRESHOLD, MEDIUM_ARRAY_THRESHOLD, len(medium_arrays)) 
        logger.info("       Large (≥%dMB): %d arrays - streaming mode", MEDIUM_ARRAY_THRESHOLD, len(large_arrays))
        
        logger.info(" Found %d processable arrays, total estimated size: %.1fMB", len(sorted_arrays), total_estimated_mb)
        logger.info("    Performance distribution: %d small, %d medium, %d large arrays", len(small_arrays), len(medium_arrays), len(large_arrays))
        if skipped_arrays:
            logger.info("   Skipped: %d arrays (%.1fMB) - exceed %dMB dynamic limit", len(skipped_arrays), skipped_mb, max_array_size_mb)
        
        # Show array size breakdown with performance categories
        logger.info("\n Processable array size breakdown:")
        for i, (name, data, size_mb) in enumerate(sorted_arrays):
            if size_mb < SMALL_ARRAY_THRESHOLD:
                category, mode = "🟢", "direct"
            elif size_mb < MEDIUM_ARRAY_THRESHOLD: 
                category, mode = "🟡", "chunked"
            else:
                category, mode = "🔴", "streaming"
            logger.info("  %2d. %s %-20s %8.1fMB (%s)", i+1, category, name, size_mb, mode)
        
        if skipped_arrays:
            logger.info("\n🚫 Skipped arrays (>%sMB):", max_array_size_mb)
            for i, (name, _, size_mb) in enumerate(skipped_arrays):
                logger.info("  %2d.  %-20s %8.1fMB (too large)", i+1, name, size_mb)
        
        logger.info("\n Performance-optimized processing strategy: %d worker(s), adaptive chunking", max_workers)
        logger.info("   📈 Strategy: Size-based processing optimization")
        logger.info("   🎯 Small arrays: Direct processing for speed")
        logger.info("   ⚡ Medium arrays: Aggressive chunking to avoid performance cliff")  
        logger.info("   🌊 Large arrays: Early streaming activation at %dMB", STREAMING_THRESHOLD)
        logger.info("   📊 Dynamic limit: Arrays >%dMB use %s mode", max_array_size_mb, large_array_mode)
        
        # Processing strategy based on array mix
        if large_arrays:
            logger.info("    Large array strategy: %d arrays will use streaming mode for optimal performance", len(large_arrays))
        if len(medium_arrays) > 0:
            logger.info("    Medium array strategy: %d arrays will use optimized chunking", len(medium_arrays))
        if len(small_arrays) > 3:
            logger.info("    Small array strategy: %d arrays will use direct processing", len(small_arrays))
        
        logger.info("\n [STEP 5/7] Starting performance-optimized array processing")
        logger.info("    Processing approach: Size-adaptive methods")
        logger.info("   💾 Memory budget: %dGB container, %.1fMB available", memory_limit_gb, available_memory_mb)
        logger.info("   ⚙  Base settings: %d worker(s), %dMB base chunks", max_workers, chunk_size_mb)
        
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
                logger.info("Processing array %d/%d: %s (%.1fMB)", completed_count + 1, total_arrays, name, size_mb)
                
                # Update main progress bar description
                main_pbar.set_description(f"Processing {name} ({size_mb:.1f}MB)")
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
                    logger.info("   EMERGENCY: Memory usage (%.0fMB) exceeds 50% of %dGB limit", mem_info['rss_mb'], memory_limit_gb)
                    logger.info("      Skipping array %s (%.1fMB) to prevent SIGKILL", name, size_mb)
                    completed_count += 1
                    main_pbar.update(1)
                    continue
                
                if mem_info['rss_mb'] > (memory_limit_gb * 1024 * 0.3):  # 30% threshold
                    logger.info("  Warning: Memory usage (%.0fMB), forcing aggressive cleanup", mem_info['rss_mb'])
                    import gc
                    gc.collect()
                    mem_info = get_memory_info()
                    logger.info("   Memory after cleanup: %.0fMB", mem_info['rss_mb'])
                    
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
                    logger.info("   Progress: %d/%d | Avg: %.1fs/array | ETA: %s", completed_count, total_arrays, avg_time, eta_str)
                
                try:
                    array_start = time.perf_counter()
                    
                    # Choose processing method based on performance-optimized thresholds
                    if size_mb > max_array_size_mb:
                        logger.info("    Array exceeds dynamic limit: using %s processing", large_array_mode)
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
                    elif size_mb >= STREAMING_THRESHOLD:
                        # Force streaming for arrays ≥100MB (performance cliff identified)
                        logger.info("    Large array (≥%dMB): forcing streaming mode for optimal performance", STREAMING_THRESHOLD)
                        result = save_volume_and_metadata_streaming(
                            name, data, output_dir, s3_uri, zarr_path, 
                            timestamp, dataset_id, voxel_size, dimensions_nm, streaming_chunk_mb
                        )
                    elif size_mb >= SMALL_ARRAY_THRESHOLD:
                        # Medium arrays: optimized chunking
                        optimized_chunk_mb = min(8, chunk_size_mb)  # Smaller chunks for medium arrays
                        logger.info("    Medium array (%d-%dMB): using optimized %dMB chunks", SMALL_ARRAY_THRESHOLD, MEDIUM_ARRAY_THRESHOLD, optimized_chunk_mb)
                        result = save_volume_and_metadata(
                            name, data, output_dir, s3_uri, zarr_path, 
                            timestamp, dataset_id, voxel_size, dimensions_nm, optimized_chunk_mb
                        )
                    else:
                        # Small arrays: direct processing with standard chunks
                        logger.info("    Small array (<%dMB): direct processing", SMALL_ARRAY_THRESHOLD)
                        result = save_volume_and_metadata(
                            name, data, output_dir, s3_uri, zarr_path, 
                            timestamp, dataset_id, voxel_size, dimensions_nm, chunk_size_mb
                        )
                    
                    array_time = time.perf_counter() - array_start
                    completed_count += 1
                    successful_count += 1
                    
                    category = "🔴" if size_mb >= MEDIUM_ARRAY_THRESHOLD else "🟡" if size_mb >= SMALL_ARRAY_THRESHOLD else "🟢"
                    logger.info(" [%d/%d] %s %s completed in %.1fs", completed_count, total_arrays, category, name, array_time)
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
                        logger.info("    Progress Summary: %d/%d arrays | %.2f arrays/min | Memory: %.0fMB", completed_count, total_arrays, rate, mem_info['rss_mb'])
                    
                    # Force cleanup after each array to prevent memory accumulation
                    import gc
                    gc.collect()
                
                except Exception as e:
                    array_time = time.perf_counter() - array_start if 'array_start' in locals() else 0
                    completed_count += 1
                    failed_count += 1
                    
                    category = "🔴" if size_mb >= MEDIUM_ARRAY_THRESHOLD else "🟡" if size_mb >= SMALL_ARRAY_THRESHOLD else "🟢"
                    logger.error(" [%d/%d] %s FAILED '%s' (%.1fMB) after %.1fs", completed_count, total_arrays, category, name, size_mb, array_time)
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
            logger.info("   Skipped (too large): %d arrays (%.1fMB)", len(skipped_arrays), skipped_mb)
        logger.info("    Success rate: %.1f%%", (successful_count/total_arrays*100))
        
        logger.info("\n⏱  TIMING BREAKDOWN:")
        logger.info("    Array processing time: %.1fs (%.1fmin)", processing_time, processing_time/60)
        logger.info("    Total pipeline time: %.1fs (%.1fmin)", pipeline_time, pipeline_time/60)
        logger.info("    Average per array: %.1fs", processing_time/total_arrays)
        if successful_count > 0:
            logger.info("    Average per success: %.1fs", processing_time/successful_count)
        
        logger.info("\n DATA SUMMARY:")
        logger.info("    Data processed: %.2f GB", total_data_gb)
        logger.info("    Throughput: %.1f GB/hour", throughput)
        logger.info("    Processing rate: %.1f arrays/min", successful_count/(processing_time/60))
        
        final_mem = get_memory_info()
        logger.info("\n MEMORY MANAGEMENT:")
        logger.info("    Final memory usage: %.1fMB", final_mem['rss_mb'])
        logger.info("    Container limit: %.0fMB", memory_limit_gb*1024)
        logger.info("    Peak utilization: %.1f%%", (final_mem['rss_mb']/(memory_limit_gb*1024)*100))
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
