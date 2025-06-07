"""
Data processing functions for OpenOrganelle Zarr data.
Contains memory-intensive computation and data transformation functions.
"""

import os
import time
import numpy as np
import dask
import dask.array as da
from tqdm import tqdm
import logging

from .helpers import get_memory_info, get_cpu_info, estimate_memory_usage

# Configure logging
logger = logging.getLogger(__name__)


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


def save_volume_and_metadata_streaming(name: str, data: da.Array, output_dir: str, s3_uri: str, internal_path: str, timestamp: str, dataset_id: str, voxel_size: dict, dimensions_nm: dict, chunk_size_mb: int) -> str:
    """
    Stream very large arrays to disk chunk-by-chunk to avoid memory issues.
    Uses disk spilling and progressive processing for arrays >500MB.
    """
    from .helpers import write_metadata_stub, save_metadata_atomically, estimate_memory_usage, get_memory_info
    
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
            # Conservative worker count for 2GB container - prioritize memory safety
            max_safe_workers = int(os.environ.get('STREAMING_WORKERS', '2'))  # Default to 2, not 4
            optimal_workers = min(cpu_count, max_safe_workers)  # Cap for memory safety
            
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
                
                # Memory check before starting intensive operation
                pre_stream_mem = get_memory_info()
                logger.info("      Pre-streaming memory: %sMB RSS", pre_stream_mem['rss_mb']:.1f)
                if pre_stream_mem['rss_mb'] > 1800:  # 90% of 2GB
                    logger.info("      WARNING: High memory usage before streaming! Consider reducing workers/chunks.")
                
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
                    
                    # Progress tracking with tqdm and memory monitoring
                    with tqdm(total=estimated_chunks, desc="     💿 Writing Zarr chunks", 
                             unit="chunks", bar_format="{desc}: {n_fmt}/{total_fmt} chunks |{bar}| [{elapsed}<{remaining}, {rate_fmt}]",
                             position=1, leave=False) as chunk_pbar:
                        
                        # Memory monitoring during computation
                        mid_stream_mem = get_memory_info()
                        logger.info("      Mid-streaming memory: %sMB RSS", mid_stream_mem['rss_mb']:.1f)
                        
                        # Compute with the current dask configuration
                        da.compute(store_result)
                        
                        # Final memory check
                        post_stream_mem = get_memory_info()
                        logger.info("      Post-streaming memory: %sMB RSS", post_stream_mem['rss_mb']:.1f)
                        
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
        import traceback
        traceback.print_exc()
        return f"❌ Failed to stream {name}: {e}"