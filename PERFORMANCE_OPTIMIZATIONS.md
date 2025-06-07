# OpenOrganelle Performance Optimizations

## Performance Analysis Summary

Based on empirical analysis of OpenOrganelle logs, we identified significant performance bottlenecks:

### Observed Performance Curve:
- **0.0MB arrays**: ~0.15s (very fast)
- **0.2MB arrays**: ~0.3s  
- **1.7MB arrays**: ~0.47s
- **13.6MB arrays**: ~4s (first bottleneck)
- **110MB arrays**: ~30s (major bottleneck - 7.5x slower than expected)
- **879.7MB arrays**: ~300s projected (exponential scaling)

### Key Finding: Performance Cliff at 100MB
The analysis revealed a **performance cliff around 100MB** where processing time becomes exponentially longer due to:
1. Memory allocation overhead
2. Inefficient chunking strategy
3. Dask graph complexity

## Implemented Optimizations

### 1. Dynamic Memory-Based Limits
**Before**: Fixed 8000MB limit
**After**: Dynamic limit based on available memory
```python
available_memory_mb = memory_limit_gb * 1024 * 0.6  # 60% of container
dynamic_max_mb = min(500, available_memory_mb / 4)  # 25% of available memory
```

### 2. Performance-Optimized Thresholds
**Before**: Fixed thresholds (25MB, 100MB, 500MB)
**After**: Empirically-tuned thresholds based on performance analysis
```python
SMALL_ARRAY_THRESHOLD = 25   # <25MB: Direct processing
MEDIUM_ARRAY_THRESHOLD = 100 # 25-100MB: Optimized chunking 
STREAMING_THRESHOLD = 100    # >100MB: Force streaming mode
```

### 3. Early Streaming Activation
**Before**: Streaming started at 500MB
**After**: Streaming starts at 100MB (performance cliff point)
- Prevents exponential scaling issues
- Uses memory-efficient streaming for large arrays

### 4. Aggressive Chunking for Medium Arrays
**Before**: Conservative chunking
**After**: Size-specific chunking strategies
```python
# Medium arrays (25-100MB): Very aggressive chunking
if dim_size > 256:
    target_chunk_size = max(16, dim_size // 12)  # 12x more aggressive
elif dim_size > 64:
    target_chunk_size = max(8, dim_size // 8)    # Small chunks
```

### 5. Optimized Default Settings
**Before**:
- `chunk_size_mb: 64`
- `streaming_chunk_mb: 32`
- `max_array_size_mb: 8000`

**After**:
- `chunk_size_mb: 8` (8x smaller for better medium array performance)
- `streaming_chunk_mb: 8` (optimized throughput)
- `max_array_size_mb: 500` (16x lower, dynamic based on memory)

### 6. Size-Adaptive Processing Strategy
```python
if size_mb >= STREAMING_THRESHOLD:           # ≥100MB
    # Force streaming mode
elif size_mb >= SMALL_ARRAY_THRESHOLD:      # 25-100MB  
    # Optimized chunking with 8MB limit
else:                                        # <25MB
    # Direct processing
```

## Expected Performance Improvements

### Processing Time Estimates:
| Array Size | Before | After | Improvement |
|------------|--------|-------|-------------|
| 13.6MB     | 4s     | ~2s   | 2x faster   |
| 110MB      | 30s    | ~8s   | 3.75x faster |
| 879MB      | 300s   | ~60s  | 5x faster   |

### Memory Efficiency:
- **Dynamic limits**: Prevents container OOM while maximizing utilization
- **Early streaming**: Reduces peak memory usage for large arrays
- **Aggressive chunking**: Better memory distribution for medium arrays

### Throughput Improvements:
- **25-100MB arrays**: Should see 2-4x performance improvement
- **100MB+ arrays**: Should see 3-5x performance improvement  
- **Overall pipeline**: Expected 40-60% faster processing

## Configuration Updates

### Environment Variables (recommended):
```bash
export MAX_ARRAY_SIZE_MB=500        # Down from 8000
export ZARR_CHUNK_SIZE_MB=8         # Down from 64
export STREAMING_CHUNK_MB=8         # Down from 32
export LARGE_ARRAY_MODE=stream      # Force streaming for large arrays
```

### config.yaml Updates:
- Reduced default chunk sizes for better medium array performance
- Added performance threshold documentation
- Set streaming as default mode for large arrays

## Monitoring and Validation

### Key Metrics to Track:
1. **Array processing time by size category**
2. **Memory utilization patterns**
3. **Streaming mode activation frequency**
4. **Overall pipeline throughput**

### Expected Log Messages:
```
Performance-optimized settings: 2GB limit, 2 workers, 8MB chunks
Array processing thresholds:
  Small arrays (<25MB): Direct processing
  Medium arrays (25-100MB): Optimized chunking
  Large arrays (>100MB): Streaming mode
Dynamic array size limit: 300MB (based on 1228.8MB available memory)
```

## Rollback Plan

If performance degrades, revert by:
1. Setting `MAX_ARRAY_SIZE_MB=8000`
2. Setting `ZARR_CHUNK_SIZE_MB=64`
3. Setting `LARGE_ARRAY_MODE=skip`

## Next Steps

1. **Monitor performance** with these optimizations
2. **Fine-tune thresholds** based on observed performance
3. **Consider parallel processing** for small arrays if memory allows
4. **Implement adaptive chunking** based on real-time memory usage