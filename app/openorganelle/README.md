# OpenOrganelle Loader

This loader ingests high-resolution electron microscopy volumes from the OpenOrganelle platform via S3 and Zarr access. It provides access to cellular organelle datasets with multi-scale pyramid support.

## Overview

The OpenOrganelle loader connects to S3-hosted Zarr datasets to download and process multi-resolution electron microscopy volumes. It supports anonymous S3 access and handles complex Zarr metadata for efficient data access.

## Data Source

- **Repository**: OpenOrganelle (Janelia Research Campus)
- **Default Dataset**: JRC_MUS-NACC-2 (Mouse Nucleus Accumbens)
- **Access Method**: S3 + Zarr format
- **Storage**: AWS S3 (janelia-cosem-datasets bucket)
- **Website**: https://openorganelle.janelia.org/

## Configuration

### Default Parameters

```python
s3_uri = "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr"
zarr_path = "recon-2/em"                         # Internal Zarr path
dataset_id = "jrc_mus-nacc-2"                    # Dataset identifier
output_dir = "zarr_volume"                       # Local output directory
s3_anonymous = True                              # Anonymous S3 access
max_workers = 4                                  # Parallel processing threads
```

### Configurable Parameters

The loader supports configuration via `lib.loader_config.OpenOrganelleConfig`:

```python
from lib.loader_config import OpenOrganelleConfig

config = OpenOrganelleConfig(
    s3_uri="s3://janelia-cosem-datasets/jrc_hela-2/jrc_hela-2.zarr",
    zarr_path="recon-1/em",
    dataset_id="jrc_hela-2",
    output_dir="./data/openorganelle",
    voxel_size={"x": 4.0, "y": 4.0, "z": 4.0},
    s3_anonymous=True,
    max_workers=2,
    timeout_seconds=1800
)
```

## Usage

### Basic Usage

```bash
cd app/openorganelle
python main.py
```

### Programmatic Usage

```python
from app.openorganelle.main import main as load_openorganelle
from lib.loader_config import OpenOrganelleConfig

# Use default configuration
result = load_openorganelle()

# Use custom configuration  
config = OpenOrganelleConfig(
    dataset_id="jrc_hela-2",
    zarr_path="recon-1/em"
)
result = load_openorganelle_with_config(config)
```

### Docker Usage

```bash
# Build and run OpenOrganelle loader
docker compose up --build openorganelle

# Run with custom dataset
docker run -e DATASET_ID=jrc_hela-2 openorganelle-loader
```

## Processing Pipeline

1. **S3 Connection**: Establish anonymous S3 filesystem connection
2. **Zarr Discovery**: Scan S3 bucket for available Zarr arrays
3. **Metadata Parsing**: Extract Zarr metadata (consolidated or individual)
4. **Array Selection**: Select appropriate resolution level and arrays
5. **Parallel Download**: Download Zarr chunks using multiple workers
6. **Array Assembly**: Reconstruct full arrays from chunks
7. **Format Conversion**: Convert to numpy arrays
8. **Metadata Generation**: Create v2.0 schema-compliant metadata using MetadataManager
9. **Validation**: Automatic validation against JSON schema during metadata creation
10. **Storage**: Save volumes and validated metadata

## Zarr Structure

OpenOrganelle datasets use hierarchical Zarr organization:

```
dataset.zarr/
├── .zgroup                    # Root group metadata
├── .zattrs                    # Root attributes
├── recon-1/                   # Reconstruction level 1
│   ├── em/                    # EM volume
│   │   ├── .zarray           # Array metadata
│   │   ├── .zattrs           # Array attributes
│   │   └── chunks/           # Data chunks
│   ├── labels/               # Segmentation labels
│   └── predictions/          # ML predictions
├── recon-2/                   # Reconstruction level 2 (higher res)
└── neuroglancer/             # Neuroglancer metadata
```

## Multi-Scale Support

OpenOrganelle datasets include multiple resolution levels:

- **s0**: Full resolution (highest detail)
- **s1**: 2x downsampled
- **s2**: 4x downsampled
- **s3**: 8x downsampled

The loader defaults to `s1` for optimal balance of detail and download size.

## Output Structure

```
data/openorganelle/
├── fibsem-int16_s8_20250607_052231.npy                # Processed Zarr array
└── metadata_fibsem-int16_s8_20250607_052231.json      # v2.0 metadata record
```

## Metadata Schema (v2.0)

The loader generates metadata following the v2.0 standardized schema using the `MetadataManager` library:

```json
{
  "id": "uuid-generated-automatically",
  "source": "openorganelle",
  "source_id": "jrc_mus-nacc-2", 
  "status": "complete",
  "created_at": "2025-06-07T05:22:31.595803Z",
  "updated_at": "2025-06-07T05:22:33.095014Z",
  "metadata": {
    "core": {
      "description": "Array 'fibsem-int16/s8' from OpenOrganelle Zarr S3 store",
      "volume_shape": [2, 9, 10],
      "data_type": "int16",
      "modality": "EM"
    },
    "technical": {
      "file_size_bytes": 360,
      "sha256": "d3df611a0ed2e328b050d285287637c60643ba96ec09e4aaefaad7f2cd114b77",
      "compression": "zarr",
      "chunk_size": [2, 9, 10],
      "global_mean": 0.0,
      "processing_time_seconds": 0.02,
      "chunk_strategy": "direct_compute",
      "dimensions_nm": [1669.44, 10080, 10384]
    },
    "provenance": {
      "download_url": "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr",
      "internal_zarr_path": "recon-2/em/fibsem-int16/s8"
    }
  },
  "files": {
    "metadata": "/app/data/openorganelle/metadata_fibsem-int16_s8_20250607_052231.json",
    "volume": "/app/data/openorganelle/fibsem-int16_s8_20250607_052231.npy"
  },
  "voxel_size_nm": {
    "x": 4.0,
    "y": 4.0,
    "z": 2.96
  },
  "imaging_start_date": "Mon Mar 09 2015",
  "additional_metadata": {
    "cosem_metadata": {
      "organism": "Mus musculus",
      "sample": "nucleus accumbens",
      "protocol": "FIB-SEM serial sectioning",
      "institution": "Janelia Research Campus",
      "dataset_version": "1.0",
      "array_path": "recon-2/em/fibsem-int16/s8"
    }
  }
}
```

**Key v2.0 Features:**
- **Schema Compliance**: Validated against `schemas/metadata_schema.json`
- **MetadataManager Integration**: Uses standardized library for metadata generation
- **UUID Generation**: Unique identifier for each record
- **Timestamp Tracking**: Created and updated timestamps in ISO format
- **File Path Tracking**: Complete file location mapping for volume and metadata
- **Zarr-specific Metadata**: Rich Zarr array metadata with chunk strategy and processing times
- **Dimensions Compliance**: Fixed dimensions_nm format (array instead of dict) for schema compliance

## S3 Access Patterns

### Anonymous Access
```python
import s3fs

# Anonymous S3 access (default)
fs = s3fs.S3FileSystem(anon=True)
zarr_store = s3fs.S3Map("janelia-cosem-datasets/dataset.zarr", s3=fs)
```

### Authenticated Access
```python
# For private datasets (if needed)
fs = s3fs.S3FileSystem(
    key="ACCESS_KEY",
    secret="SECRET_KEY"
)
```

## Dataset Catalog

Available datasets include:

- **jrc_mus-nacc-2**: Mouse nucleus accumbens
- **jrc_hela-2**: HeLa cells  
- **jrc_macrophage-2**: Macrophage cells
- **jrc_jurkat-1**: Jurkat T cells
- **jrc_cos7-1**: COS7 cells

Each dataset contains multiple reconstruction levels and data types.

## Error Handling

### S3 Connectivity Issues
- Automatic retry with exponential backoff
- Anonymous access validation
- Bucket permission checking

### Zarr Format Issues  
- Consolidated metadata fallback
- Chunk availability verification
- Compression format support

### Memory Management
- Chunked processing for large arrays
- Parallel worker memory limiting
- Temporary file cleanup

## Testing

### Unit Tests

```bash
# Run OpenOrganelle-specific unit tests
python run_tests.py loader openorganelle

# Test S3 mocking
pytest tests/test_unit_openorganelle.py::TestOpenOrganelleLoader::test_s3_connection
```

### Integration Tests

```bash
# Test with real S3 access (requires network)
python run_tests.py integration --loader openorganelle

# Test small dataset download
pytest tests/test_integration.py -k "openorganelle and small"
```

### Test Configuration

```yaml
development:
  testing:
    test_configs:
      openorganelle:
        timeout_seconds: 300
        max_workers: 2
        s3_anonymous: true
        enable_downloads: false  # Mock S3 access in tests
```

## Performance Characteristics

- **Download Speed**: 100-500 MB/s (depending on chunk size and parallelism)
- **Memory Usage**: ~1.5x final array size during processing
- **Parallel Efficiency**: Scales well with `max_workers` up to ~8 threads
- **Chunk Optimization**: 64³ chunks provide good balance of throughput and latency

## Advanced Usage

### Custom Resolution Selection

```python
# Download highest resolution data
config = OpenOrganelleConfig(
    zarr_path="recon-2/em/s0",  # Full resolution
    max_workers=8  # More parallelism for larger data
)
```

### Multi-Array Processing

```python
# Download both EM and labels
arrays_to_process = [
    "recon-2/em/s1",
    "recon-2/labels/s1"
]

for array_path in arrays_to_process:
    config = OpenOrganelleConfig(zarr_path=array_path)
    result = load_openorganelle_with_config(config)
```

### Chunk-wise Processing

```python
# For very large arrays, process in chunks
import zarr
import s3fs

fs = s3fs.S3FileSystem(anon=True)
store = s3fs.S3Map("janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr", s3=fs)
z = zarr.open(store)

# Process in 512³ chunks
chunk_size = 512
for z_start in range(0, z.shape[0], chunk_size):
    chunk = z[z_start:z_start+chunk_size, :chunk_size, :chunk_size]
    # Process chunk...
```

## Troubleshooting

### Common Issues

**S3 Access Denied**
```bash
# Verify bucket access
aws s3 ls s3://janelia-cosem-datasets/ --no-sign-request
```

**Zarr Path Not Found**
```python
# List available arrays
import s3fs
import zarr

fs = s3fs.S3FileSystem(anon=True)
store = s3fs.S3Map("janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr", s3=fs)
z = zarr.open(store)
print(z.tree())  # Show Zarr hierarchy
```

**Memory Issues with Large Arrays**
- Reduce `max_workers` to limit memory usage
- Use chunked processing for arrays >10GB
- Monitor available RAM during processing

**Slow Downloads**
- Increase `max_workers` (try 8-16 for large datasets)
- Use appropriate resolution level (s1 vs s0)
- Check network bandwidth to AWS

### Debug Mode

```python
config = OpenOrganelleConfig(verbose=True, debug_mode=True)
# Enables S3 operation logging and chunk download progress
```

## Data Citation

When using OpenOrganelle data, please cite:

```
Heinrich, L. et al. Whole-cell organelle segmentation in volume electron microscopy. Nature 599, 141–146 (2021).
```

## Dependencies

```
zarr>=2.12.0       # Zarr array format
s3fs>=2023.1.0     # S3 filesystem interface
dask>=2023.1.0     # Parallel processing
numpy>=1.21.0      # Array processing
tqdm>=4.64.0       # Progress bars
```

## Related Documentation

- [OpenOrganelle Portal](https://openorganelle.janelia.org/)
- [COSEM Project](https://www.janelia.org/project-team/cosem)
- [Zarr Format](https://zarr.readthedocs.io/)
- [S3FS Documentation](https://s3fs.readthedocs.io/)
- [AWS S3 Anonymous Access](https://docs.aws.amazon.com/AmazonS3/latest/userguide/anonymous-requests.html)