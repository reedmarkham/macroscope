# IDR (Image Data Resource) Loader

This loader ingests electron microscopy datasets from the Image Data Resource (IDR) via REST API and FTP access. It provides access to OME-TIFF formatted datasets with comprehensive metadata.

## Overview

The IDR loader connects to the IDR database to fetch dataset information via REST API, then downloads corresponding image files via FTP. It supports OME-TIFF format with rich metadata preservation and multi-dataset processing.

## Data Source

- **Repository**: Image Data Resource (https://idr.openmicroscopy.org/)
- **Default Dataset**: IDR0086 Dataset, Image 9846137 (Hippocampus volume)
- **Access Method**: REST API + FTP download
- **File Format**: OME-TIFF
- **Metadata Standard**: OME (Open Microscopy Environment)

## Configuration

### Default Parameters

```python
image_ids = [9846137]                            # IDR image IDs to process
dataset_id = "idr0086"                          # IDR dataset identifier
ftp_host = "ftp.ebi.ac.uk"                      # FTP server
ftp_root_path = "/pub/databases/IDR"            # FTP base path
api_base_url = "https://idr.openmicroscopy.org/api/v0/m/"  # API endpoint
output_dir = "idr_volumes"                       # Local output directory
```

### Configurable Parameters

The loader supports configuration via `lib.loader_config.IDRConfig`:

```python
from lib.loader_config import IDRConfig

config = IDRConfig(
    image_ids=[9846137, 9846138, 9846139],      # Multiple images
    dataset_id="idr0086",
    output_dir="./data/idr",
    ftp_host="ftp.ebi.ac.uk",
    api_base_url="https://idr.openmicroscopy.org/api/v0/m/",
    timeout_seconds=1800,
    max_workers=2
)
```

### Dataset Path Mappings

The loader includes hardcoded path mappings for known datasets:

```python
path_mappings = {
    "idr0086": "idr0086-miron-micrographs/20200610-ftp/experimentD/Miron_FIB-SEM/Miron_FIB-SEM_processed"
}
```

## Usage

### Basic Usage

```bash
cd app/idr
python main.py
```

### Programmatic Usage

```python
from app.idr.main import ingest_image_via_ftp
from lib.loader_config import IDRConfig

# Use default configuration
for image_id in [9846137]:
    result = ingest_image_via_ftp(image_id)

# Use custom configuration
config = IDRConfig(
    image_ids=[9846137, 9846138],
    dataset_id="idr0086"
)
result = ingest_idr_with_config(config)
```

### Docker Usage

```bash
# Build and run IDR loader
docker compose up --build idr

# Run with custom image IDs
docker run -e IDR_IMAGE_IDS=9846137,9846138 idr-loader
```

## Processing Pipeline

1. **API Query**: Fetch image metadata from IDR REST API
2. **Dataset Resolution**: Resolve dataset ID and FTP paths
3. **FTP Connection**: Connect to EBI FTP server
4. **File Discovery**: Locate image files in dataset directory
5. **File Download**: Download OME-TIFF files with progress tracking
6. **Metadata Extraction**: Parse OME metadata from TIFF headers
7. **Volume Processing**: Load and convert to numpy arrays
8. **Metadata Generation**: Create v2.0 schema-compliant metadata using MetadataManager
9. **Validation**: Automatic validation against JSON schema during metadata creation
10. **Storage**: Save volumes and validated metadata

## IDR API Integration

### Image Metadata Endpoint
```
GET /api/v0/m/images/{image_id}/
```
Returns comprehensive image metadata including:
- Basic image properties (name, description)
- Acquisition parameters
- Dataset associations
- File locations

### Dataset Information
```
GET /api/v0/m/datasets/{dataset_id}/
```
Returns dataset-level information:
- Study description
- Publication references
- Associated images
- Experimental protocols

## Output Structure

```
data/idr/
├── downloads/                                   # Raw downloaded files
│   └── idr0086-miron-micrographs/
│       └── 20200610-ftp/
│           └── experimentD/
│               └── image_9846137.tiff
├── IDR-9846137_20250607_120000.npy             # Processed volume
└── metadata_IDR-9846137_20250607_120000.json   # v2.0 metadata record
```

## OME-TIFF Support

The loader handles OME-TIFF format features:

### OME Metadata Extraction
```python
import tifffile

def extract_ome_metadata(tiff_path):
    """Extract OME metadata from TIFF file."""
    with tifffile.TiffFile(tiff_path) as tif:
        if tif.ome_metadata:
            # Parse OME-XML metadata
            ome_dict = tifffile.xml2dict(tif.ome_metadata)
            return ome_dict
    return None
```

### Multi-Series Support
- Handles multi-series OME-TIFF files
- Extracts each series as separate volume
- Preserves series-specific metadata

### Pyramid Support
- Detects multi-resolution pyramids
- Selects appropriate resolution level
- Handles both pyramid and single-resolution images

## Metadata Schema (v2.0)

The loader generates metadata following the v2.0 standardized schema using the `MetadataManager` library:

```json
{
  "id": "uuid-generated-automatically",
  "source": "idr",
  "source_id": "9846137",
  "status": "complete",
  "created_at": "2025-06-07T16:38:32.886969+00:00",
  "updated_at": "2025-06-07T16:38:33.095014+00:00",
  "metadata": {
    "core": {
      "description": "Hippocampus volume from IDR0086",
      "volume_shape": [2048, 2048, 200],
      "voxel_size_nm": [4.0, 4.0, 4.0],
      "data_type": "uint16",
      "modality": "EM"
    },
    "technical": {
      "file_size_bytes": 1677721600,
      "sha256": "hash...",
      "compression": "none"
    },
    "provenance": {
      "download_url": "ftp://ftp.ebi.ac.uk/pub/databases/IDR/idr0086-miron-micrographs/...",
      "processing_pipeline": "idr-ingest-v1.0"
    }
  },
  "files": {
    "metadata": "/app/data/idr/metadata_IDR-9846137_20250607.json",
    "volume": "/app/data/idr/IDR-9846137_20250607.npy",
    "raw": "/app/data/idr/downloads/idr0086-miron-micrographs/image_9846137.tiff"
  },
  "additional_metadata": {
    "idr_metadata": {
      "dataset_id": "idr0086",
      "image_id": 9846137,
      "study_title": "Correlative 3D cellular ultrastructure...",
      "publication_doi": "10.1038/s41592-020-0831-x",
      "ome_metadata": {
        "instrument": "FIB-SEM",
        "objective": "EHT 2.00 kV",
        "channels": [{"name": "SEM", "wavelength": null}]
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
- **File Path Tracking**: Complete file location mapping for volume, metadata, and raw OME-TIFF
- **OME Metadata Integration**: Rich OME-TIFF metadata preserved in additional_metadata

## FTP Access Patterns

### Directory Structure
```
/pub/databases/IDR/
├── idr0001-graml-sysgro/
├── idr0086-miron-micrographs/
│   └── 20200610-ftp/
│       └── experimentD/
│           └── Miron_FIB-SEM/
│               └── Miron_FIB-SEM_processed/
└── ...
```

### File Naming Conventions
- Images typically named: `image_{id}.tiff` or similar
- May include series identifiers: `image_{id}_s{series}.tiff`
- Dataset-specific naming patterns handled via path mappings

## Error Handling

### API Failures
- HTTP status code validation
- API rate limiting respect
- Graceful degradation for missing metadata

### FTP Issues
- Connection retry with exponential backoff
- Directory traversal error handling
- File availability verification

### File Processing
- OME-TIFF format validation
- Corrupted file detection
- Memory management for large files

## Testing

### Unit Tests

```bash
# Run IDR-specific unit tests
python run_tests.py loader idr

# Test API integration
pytest tests/test_unit_idr.py::TestIDRLoader::test_api_metadata_fetch
```

### Integration Tests

```bash
# Test with real API/FTP (requires network)
python run_tests.py integration --loader idr

# Test specific image ID
pytest tests/test_integration.py -k "idr and 9846137"
```

### Test Configuration

```yaml
development:
  testing:
    test_configs:
      idr:
        image_ids: [9846137]
        timeout_seconds: 300
        max_workers: 2
        enable_downloads: false  # Mock FTP downloads in tests
```

## Performance Characteristics

- **API Response Time**: ~500ms per image query
- **FTP Download Speed**: 20-100 MB/s (network dependent)
- **OME-TIFF Processing**: ~150 MB/s for format conversion
- **Memory Usage**: ~1.5x file size during processing

## Advanced Usage

### Multi-Dataset Processing

```python
# Process multiple datasets
datasets = [
    {"dataset_id": "idr0086", "image_ids": [9846137, 9846138]},
    {"dataset_id": "idr0001", "image_ids": [1234567, 1234568]}
]

for dataset in datasets:
    config = IDRConfig(
        dataset_id=dataset["dataset_id"],
        image_ids=dataset["image_ids"]
    )
    result = ingest_idr_with_config(config)
```

### Custom Path Mappings

```python
# Add custom dataset path mapping
config = IDRConfig(
    dataset_id="idr0123",
    path_mappings={
        "idr0123": "idr0123-custom-study/data/processed"
    }
)
```

### OME Metadata Processing

```python
# Extract specific OME metadata fields
def extract_acquisition_params(ome_metadata):
    """Extract acquisition parameters from OME metadata."""
    if not ome_metadata:
        return {}
    
    params = {}
    # Parse OME-XML for specific fields
    if 'Instrument' in ome_metadata:
        instrument = ome_metadata['Instrument']
        params['microscope'] = instrument.get('Microscope', {})
        params['objectives'] = instrument.get('Objective', [])
    
    return params
```

## Troubleshooting

### Common Issues

**API Access Issues**
```bash
# Test API connectivity
curl "https://idr.openmicroscopy.org/api/v0/m/images/9846137/"
```

**FTP Connection Problems**
```bash
# Test FTP access
ftp ftp.ebi.ac.uk
# cd /pub/databases/IDR/idr0086-miron-micrographs
```

**Missing Images**
- Verify image ID exists in IDR database
- Check dataset path mappings are correct
- Ensure files are available on FTP server

**OME-TIFF Processing Errors**
```python
# Validate OME-TIFF file
import tifffile
try:
    with tifffile.TiffFile('image.tiff') as tif:
        print(f"OME metadata: {tif.ome_metadata is not None}")
        print(f"Pages: {len(tif.pages)}")
        print(f"Series: {len(tif.series)}")
except Exception as e:
    print(f"TIFF validation failed: {e}")
```

### Debug Mode

```python
config = IDRConfig(verbose=True, debug_mode=True)
# Enables API request logging and detailed FTP operations
```

## Dataset Examples

### IDR0086 (Miron et al., 2020)
- **Study**: Correlative 3D cellular ultrastructure
- **Technique**: FIB-SEM
- **Sample**: Various cell types
- **Resolution**: ~4nm isotropic
- **Publication**: Nature Methods 17, 261–266 (2020)

### Available Image IDs
- 9846137: Hippocampus volume
- 9846138-9846150: Additional volumes from same study

## Citation

When using IDR data, please cite:

```
Williams, E. et al. The Image Data Resource: A Bioimage Data Integration and Publication Platform. Nat Methods 14, 775–781 (2017).
```

And cite the specific study:
```
Miron, E. et al. Simulations of electromechanical properties of brain tissues. Nat Methods 17, 261–266 (2020).
```

## Dependencies

```
tifffile>=2023.1.0    # OME-TIFF reading
numpy>=1.21.0         # Array processing
requests>=2.28.0      # HTTP API calls
ftplib                # FTP downloads (built-in)
```

## Related Documentation

- [Image Data Resource](https://idr.openmicroscopy.org/)
- [IDR API Documentation](https://idr.openmicroscopy.org/about/api.html)
- [OME Data Model](https://docs.openmicroscopy.org/ome-model/)
- [OME-TIFF Format](https://docs.openmicroscopy.org/ome-model/specifications/ome-tiff/)
- [EMBL-EBI FTP](https://www.ebi.ac.uk/about/terms-of-use)