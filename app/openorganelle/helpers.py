"""
Helper functions for OpenOrganelle Zarr data ingestion.
This module contains utility functions extracted from main.py to improve maintainability.
"""

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
import logging

# Configure logging
logger = logging.getLogger(__name__)


def load_zarr_arrays_from_s3(bucket_uri: str, internal_path: str) -> dict:
    """Load Zarr arrays from S3 bucket with optimized metadata loading."""
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
    """Create summary statistics for a dask array."""
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


def write_metadata_stub(name, npy_path, metadata_path, s3_uri, internal_path, dataset_id: str, voxel_size: dict, dimensions_nm: dict) -> dict:
    """Create initial metadata stub for an array."""
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
    """Save metadata atomically using temporary file."""
    tmp_path = metadata_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp_path, metadata_path)


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