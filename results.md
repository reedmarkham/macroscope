# Loader Performance Analysis Results

## Loader Performance Summary

### Processing Times & Dataset Sizes

**IDR Loader:**
- **Processing Time**: ~3.6 seconds (created_at: 21:18:14, updated_at: 21:18:17)
- **Dataset**: IDR image 9846137 (Figure_S3B_FIB-SEM_U2OS)
- **File Size**: 159.9 MB
- **Volume**: 184 × 775 × 1121 voxels
- **Performance**: ~44.4 MB/s processing rate

**FlyEM Loader:**
- **Processing Time**: ~8.2 seconds (created_at: 21:19:44, updated_at: 21:19:52)
- **Dataset**: Random crop from hemibrain connectome
- **File Size**: 1.0 GB (1,000,000,000 bytes)
- **Volume**: 1000 × 1000 × 1000 voxels
- **Performance**: ~122 MB/s processing rate

**EBI Loader:**
- **Processing Time**: ~1 second (created_at: 21:20:56, updated_at: 21:20:57)
- **Dataset**: EMPIAR-11759 (zebrafish retina SBF-SEM)
- **File Size**: 30.2 MB
- **Volume**: 5500 × 5496 voxels (2D slice)
- **Performance**: ~30.2 MB/s processing rate

**EPFL Loader:**
- **Processing Time**: ~4 minutes, 26 seconds (based on file timestamp difference)
- **Dataset**: CA1 hippocampus section
- **File Size**: 3.35 GB (3,350,200,320 bytes)
- **Volume**: 1065 × 1536 × 2048 voxels
- **Performance**: ~12.6 MB/s processing rate

**OpenOrganelle Loader:**
- **Processing Time**: 11 minutes, 1 second (661.93 seconds for s0 array)
- **Dataset**: Mouse nucleus accumbens (jrc_mus-nacc-2)
- **Volume**: 564 × 2520 × 2596 voxels for s0 scale
- **Processing Strategy**: Streaming with disk spill (34,352 chunks)
- **Performance**: Advanced chunking optimization with real-time memory management

### Performance Rankings

1. **FlyEM**: Fastest at 122 MB/s (1GB in 8.2s)
2. **IDR**: 44.4 MB/s (160MB in 3.6s) 
3. **EBI**: 30.2 MB/s (30MB in 1s)
4. **EPFL**: 12.6 MB/s (3.35GB in 4m26s)
5. **OpenOrganelle**: Complex processing with adaptive chunking (multiple arrays processed with streaming optimization)

### Test Execution Performance

**Overall Test Suite**: 36.3 seconds for 236 tests
- **216 passed**, 14 failed (MetadataManager mocking issues), 6 skipped
- **29% overall coverage** improvement across all loaders

**Slowest Test Operations**:
- OpenOrganelle memory estimation: 14.3 seconds
- IDR OME-TIFF loading: 2.6 seconds
- EPFL data quality checks: 2.0 seconds
- Integration metadata processing: 2.1 seconds

### Performance Summary Table

| Loader | Run Time | Dataset Size | Average Speed | Volumes Processed |
|--------|----------|--------------|---------------|-------------------|
| **FlyEM** | 8.2s | 1.0 GB | 122 MB/s | 1 |
| **IDR** | 3.6s | 159.9 MB | 44.4 MB/s | 1 |
| **EBI** | 1.0s | 30.2 MB | 30.2 MB/s | 1 |
| **EPFL** | 4m 26s | 3.35 GB | 12.6 MB/s | 1 |
| **OpenOrganelle** | 11m 1s* | Multi-scale | Variable† | 9 |

*Time shown for largest array (s0). Total processing time across all 9 volumes was longer.
†OpenOrganelle uses adaptive chunking with streaming optimization, making speed calculation complex.

### Analysis Notes

The performance data shows FlyEM and IDR as the most efficient loaders for their respective data sizes, while EPFL and OpenOrganelle handle much larger datasets with more complex processing requirements. The variation in processing rates reflects the different data formats, network conditions, and processing complexity of each loader.

**Data Sources:**
- Metadata files from `data/*/metadata*.json`
- Test execution logs from `test_results/junit.xml`
- Processing timestamps extracted from metadata records
- Dataset size information from technical metadata fields