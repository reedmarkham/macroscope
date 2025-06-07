# FlyEM/DVID Loader

This loader generates random crops from the FlyEM Hemibrain dataset via DVID (Distributed Versioned Image Data) REST API. It provides access to large-scale connectomics data with flexible cropping capabilities.

## Overview

The FlyEM loader connects to DVID servers to extract random volumetric crops from the Hemibrain dataset. It features intelligent bounds detection, coordinate validation, and supports reproducible crop generation for research applications.

## Data Source

- **Repository**: FlyEM Hemibrain (Janelia Research Campus)
- **Default Dataset**: Hemibrain connectome dataset
- **Access Method**: DVID REST API
- **Data Format**: Raw 8-bit grayscale volumes
- **Neuroglancer URL**: https://tinyurl.com/hemibrain-ng

## Configuration

### Default Parameters

```python
dvid_server = "http://hemibrain-dvid.janelia.org"    # DVID server URL
uuid = "a89eb3af216a46cdba81204d8f954786"            # Dataset UUID
instance = "grayscale"                               # Data instance name
crop_size = (1000, 1000, 1000)                      # Crop dimensions (Z,Y,X)
output_dir = "dvid_crops"                            # Local output directory
```

### Configurable Parameters

The loader supports configuration via `lib.loader_config.FlyEMConfig`:

```python
from lib.loader_config import FlyEMConfig

config = FlyEMConfig(
    dvid_server="http://custom-dvid-server.com",
    uuid="different-dataset-uuid",
    instance="em_data",
    crop_size=(500, 500, 500),           # Smaller crops
    output_dir="./data/flyem",
    random_seed=42,                      # Reproducible crops
    max_workers=2,
    timeout_seconds=1800
)
```

## Usage

### Basic Usage

```bash
cd app/flyem
python main.py
```

### Programmatic Usage

```python
from app.flyem.main import fetch_random_crop
from lib.loader_config import FlyEMConfig

# Use default configuration
result = fetch_random_crop()

# Use custom configuration
config = FlyEMConfig(
    crop_size=(256, 256, 256),
    random_seed=123
)
result = fetch_random_crop_with_config(config)
```

### Docker Usage

```bash
# Build and run FlyEM loader
docker compose up --build flyem

# Run with custom crop size
docker run -e CROP_SIZE=500,500,500 flyem-loader
```

## Processing Pipeline

1. **Server Connection**: Connect to DVID server and validate access
2. **Dataset Info**: Fetch dataset metadata (voxel size, bounds)
3. **Bounds Detection**: Determine valid coordinate ranges using sparsevol endpoint
4. **Coordinate Generation**: Generate random crop coordinates within bounds
5. **Data Retrieval**: Download volumetric data via raw data API
6. **Array Processing**: Convert raw bytes to numpy arrays
7. **Metadata Generation**: Create v2.0 schema-compliant metadata using MetadataManager
8. **Validation**: Automatic validation against JSON schema during metadata creation
9. **Storage**: Save crops and validated metadata with coordinate-based naming

## Output Structure

```
data/flyem/
├── crop_12000_15000_8000_1000x1000x1000_20250607_120000.npy  # Volume crop
└── metadata_crop_12000_15000_8000_1000x1000x1000_20250607_120000.json  # v2.0 metadata
```

## Coordinate System

FlyEM uses a standard 3D coordinate system:
- **X**: Width (left-right)
- **Y**: Height (top-bottom) 
- **Z**: Depth (front-back)

Coordinates are in voxel units, typically with 8nm isotropic resolution.

## Bounds Detection

The loader implements sophisticated bounds detection:

1. **Info Endpoint**: Query dataset basic information
2. **Sparsevol Endpoint**: Get non-zero data bounds
3. **Fallback Methods**: Use alternative endpoints if primary fails
4. **Validation**: Ensure crops fit within detected bounds

## Metadata Schema (v2.0)

The loader generates metadata following the v2.0 standardized schema using the `MetadataManager` library:

```json
{
  "id": "uuid-generated-automatically",
  "source": "flyem", 
  "source_id": "a89eb3af216a46cdba81204d8f954786",
  "status": "complete",
  "created_at": "2025-06-07T16:38:32.886969+00:00",
  "updated_at": "2025-06-07T16:38:33.095014+00:00",
  "metadata": {
    "core": {
      "description": "FlyEM hemibrain random crop",
      "volume_shape": [1000, 1000, 1000],
      "voxel_size_nm": [8.0, 8.0, 8.0],
      "data_type": "uint8",
      "modality": "EM"
    },
    "technical": {
      "file_size_bytes": 1000000000,
      "sha256": "hash..."
    },
    "provenance": {
      "uuid": "a89eb3af216a46cdba81204d8f954786",
      "processing_pipeline": "flyem-ingest-v1.0"
    }
  },
  "files": {
    "metadata": "/app/data/flyem/metadata_crop_12000_15000_8000_20250607.json",
    "volume": "/app/data/flyem/crop_12000_15000_8000_1000x1000x1000_20250607.npy"
  },
  "additional_metadata": {
    "dvid_metadata": {
      "server": "http://hemibrain-dvid.janelia.org",
      "instance": "grayscale", 
      "crop_coordinates": [12000, 15000, 8000],
      "crop_size": [1000, 1000, 1000],
      "dataset_bounds": {
        "min_point": [0, 0, 0],
        "max_point": [34432, 39552, 41408]
      }
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
- **DVID Integration**: Rich DVID server metadata preserved in additional_metadata

## DVID API Endpoints

### Info Endpoint
```
GET /api/node/{uuid}/{instance}/info
```
Returns dataset metadata including voxel size and basic properties.

### Raw Data Endpoint  
```
GET /api/node/{uuid}/{instance}/raw/0_1_2/{size_x}_{size_y}_{size_z}/{offset_x}_{offset_y}_{offset_z}
```
Returns raw volumetric data as binary stream.

### Sparsevol Endpoint
```
GET /api/node/{uuid}/{instance}/sparsevol-size/0
```
Returns bounds of non-zero data regions.

## Reproducible Crops

For reproducible research, use fixed random seeds:

```python
config = FlyEMConfig(
    random_seed=42,
    crop_size=(512, 512, 512)
)

# Multiple runs will generate identical crops
crop1 = fetch_random_crop_with_config(config)
crop2 = fetch_random_crop_with_config(config)
# crop1 and crop2 will have identical coordinates
```

## Error Handling

### Network Errors
- Automatic retry with exponential backoff
- Connection timeout handling
- Server unavailability detection

### Coordinate Validation
- Bounds checking before data request
- Invalid crop size detection
- Out-of-bounds coordinate handling

### Data Validation
- Array shape verification
- Data type consistency checking
- Size mismatch detection

## Testing

### Unit Tests

```bash
# Run FlyEM-specific unit tests
python run_tests.py loader flyem

# Test coordinate generation
pytest tests/test_unit_flyem.py::TestFlyEMLoader::test_crop_coordinate_generation
```

### Integration Tests

```bash
# Test with real DVID server (requires network)
python run_tests.py integration --loader flyem

# Test reproducible crops
pytest tests/test_unit_flyem.py::TestFlyEMLoader::test_random_seed_reproducibility
```

### Test Configuration

```yaml
development:
  testing:
    test_configs:
      flyem:
        crop_size: [100, 100, 100]  # Small crops for testing
        timeout_seconds: 300
        max_workers: 2
        random_seed: 42
        enable_downloads: false
```

## Performance Characteristics

- **Typical Download Speed**: 50-200 MB/s (server dependent)
- **Crop Generation Time**: ~2-10 seconds per crop
- **Memory Usage**: ~1.2x crop size during processing
- **Concurrent Requests**: Supports multiple parallel crops

## Advanced Usage

### Custom Coordinate Ranges

```python
# Generate crops from specific brain regions
config = FlyEMConfig(
    crop_size=(256, 256, 256),
    custom_bounds={
        "min_point": [10000, 15000, 5000],
        "max_point": [25000, 30000, 15000]
    }
)
```

### Batch Crop Generation

```python
# Generate multiple crops efficiently
configs = [
    FlyEMConfig(random_seed=i, crop_size=(512, 512, 512))
    for i in range(10)
]

crops = [fetch_random_crop_with_config(config) for config in configs]
```

## Troubleshooting

### Common Issues

**DVID Server Connectivity**
```bash
# Test server access
curl http://hemibrain-dvid.janelia.org/api/node/{uuid}/grayscale/info
```

**UUID Changes**
- UUIDs may change with dataset updates
- Check FlyEM website for current UUIDs
- Use latest stable UUID for production

**Bounds Detection Failures**
```python
# Manual bounds specification
config = FlyEMConfig(
    manual_bounds=True,
    dataset_bounds={"min_point": [0,0,0], "max_point": [34432,39552,41408]}
)
```

**Large Crop Timeouts**
- Reduce crop size for faster downloads
- Increase timeout_seconds for large crops
- Use chunked downloading for very large regions

### Debug Mode

```python
config = FlyEMConfig(verbose=True, debug_mode=True)
# Enables coordinate logging and detailed error reporting
```

## Data Citation

When using FlyEM data, please cite:

```
Zheng, Z. et al. A Complete Electron Microscopy Volume of the Brain of Adult Drosophila melanogaster. Cell 174, 730–743.e22 (2018).
```

## Dependencies

```
numpy>=1.21.0      # Array processing
requests>=2.28.0   # HTTP API calls
tqdm>=4.64.0       # Progress bars (if used)
```

## Related Documentation

- [FlyEM Project](https://www.janelia.org/project-team/flyem)
- [DVID Documentation](https://dvid.io/)
- [Hemibrain Dataset](https://www.janelia.org/project-team/flyem/hemibrain)
- [Neuroglancer Viewer](https://tinyurl.com/hemibrain-ng)