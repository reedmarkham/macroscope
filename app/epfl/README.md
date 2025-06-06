# EPFL CVLab Loader

This loader ingests electron microscopy datasets from the EPFL Computer Vision Lab (CVLab) via direct HTTP download. It processes TIFF stack volumes from hippocampus imaging studies.

## Overview

The EPFL loader downloads and processes TIFF stack datasets from the CVLab electron microscopy data repository. It provides straightforward single-file download with comprehensive metadata extraction.

## Data Source

- **Repository**: EPFL CVLab EM Data (https://www.epfl.ch/labs/cvlab/data/data-em/)
- **Default Dataset**: Hippocampus volume (5x5x5µm section from CA1 region)
- **Access Method**: Direct HTTP download
- **File Format**: Multi-page TIFF stack
- **Resolution**: 5nm isotropic voxels

## Configuration

### Default Parameters

```python
download_url = "https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/volumedata.tif"
output_dir = "epfl_em_data"                      # Local output directory
source_id = "EPFL-CA1-HIPPOCAMPUS"              # Dataset identifier
description = "5x5x5µm section from CA1 hippocampus region"
voxel_size_nm = [5.0, 5.0, 5.0]                 # Isotropic 5nm voxels
chunk_size_mb = 8                                # Download chunk size
```

### Configurable Parameters

The loader supports configuration via `lib.loader_config.EPFLConfig`:

```python
from lib.loader_config import EPFLConfig

config = EPFLConfig(
    download_url="https://custom-url.com/dataset.tif",
    source_id="CUSTOM-DATASET-001",
    description="Custom EM dataset description",
    voxel_size_nm=[4.0, 4.0, 4.0],      # Different voxel size
    output_dir="./data/epfl",
    chunk_size_mb=16,                     # Larger chunks
    timeout_seconds=1800,
    max_workers=2
)
```

## Usage

### Basic Usage

```bash
cd app/epfl
python main.py
```

### Programmatic Usage

```python
from app.epfl.main import ingest_epfl_tif
from lib.loader_config import EPFLConfig

# Use default configuration
result = ingest_epfl_tif()

# Use custom configuration
config = EPFLConfig(
    download_url="https://example.com/dataset.tif",
    source_id="CUSTOM-DATASET"
)
result = ingest_epfl_tif_with_config(config)
```

### Docker Usage

```bash
# Build and run EPFL loader
docker compose up --build epfl

# Run with custom URL
docker run -e EPFL_URL=https://example.com/data.tif epfl-loader
```

## Processing Pipeline

1. **URL Validation**: Verify download URL accessibility
2. **Streaming Download**: Download TIFF file with progress tracking
3. **Format Validation**: Verify TIFF format and multi-page structure
4. **Volume Loading**: Load complete TIFF stack into memory
5. **Data Processing**: Convert to standardized numpy array format
6. **Metadata Generation**: Create comprehensive metadata record
7. **Validation**: Validate against JSON schema
8. **Storage**: Save processed volume and metadata

## Output Structure

```
epfl_em_data/
├── EPFL-CA1-HIPPOCAMPUS_20240101_120000.npy              # Processed volume
├── EPFL-CA1-HIPPOCAMPUS_original_20240101_120000.tif     # Original TIFF file
└── metadata_EPFL-CA1-HIPPOCAMPUS_20240101_120000.json    # Metadata record
```

## TIFF Stack Processing

The loader handles multi-page TIFF stacks:

- **Page Reading**: Sequential page-by-page loading
- **Stack Assembly**: Combines pages into 3D volume
- **Memory Management**: Efficient handling of large stacks
- **Metadata Preservation**: Extracts TIFF metadata when available

## Metadata Schema

```json
{
  "id": "uuid",
  "source": "epfl",
  "source_id": "EPFL-CA1-HIPPOCAMPUS",
  "status": "complete", 
  "metadata": {
    "core": {
      "description": "5x5x5µm section from CA1 hippocampus region",
      "volume_shape": [1024, 1024, 165],
      "voxel_size_nm": [5.0, 5.0, 5.0],
      "data_type": "uint8",
      "modality": "EM"
    },
    "technical": {
      "file_size_bytes": 173015040,
      "sha256": "hash...",
      "compression": "none"
    },
    "provenance": {
      "download_url": "https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/volumedata.tif",
      "processing_pipeline": "epfl-ingest-v1.0"
    }
  },
  "additional_metadata": {
    "epfl_metadata": {
      "lab": "Computer Vision Lab (CVLab)",
      "institution": "École Polytechnique Fédérale de Lausanne (EPFL)",
      "tissue_type": "hippocampus",
      "brain_region": "CA1",
      "species": "rodent"
    }
  }
}
```

## Streaming Download

The loader implements efficient streaming download:

```python
def download_with_progress(url, output_path, chunk_size_mb=8):
    """Download file with progress tracking."""
    chunk_size = chunk_size_mb * 1024 * 1024
    
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_path, 'wb') as f:
            with tqdm(total=total_size, unit='B', unit_scale=True) as pbar:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        pbar.update(len(chunk))
```

## Error Handling

### Network Errors
- HTTP status code validation
- Connection timeout handling
- Retry logic for transient failures

### File Format Errors
- TIFF format validation
- Multi-page verification
- Corrupted file detection

### Memory Management
- Large file handling with streaming
- Temporary file cleanup
- Memory usage monitoring

## Testing

### Unit Tests

```bash
# Run EPFL-specific unit tests
python run_tests.py loader epfl

# Test download functionality
pytest tests/test_unit_epfl.py::TestEPFLLoader::test_streaming_download
```

### Integration Tests

```bash
# Test with real download (requires network)
python run_tests.py integration --loader epfl

# Test with small dataset
pytest tests/test_integration.py -k "epfl and small"
```

### Test Configuration

```yaml
development:
  testing:
    test_configs:
      epfl:
        timeout_seconds: 300
        max_workers: 2
        chunk_size_mb: 1  # Smaller chunks for testing
        enable_downloads: false  # Mock downloads in tests
```

## Performance Characteristics

- **Download Speed**: 20-100 MB/s (network dependent)
- **Processing Speed**: ~200 MB/s for TIFF loading
- **Memory Usage**: ~2x file size during processing
- **Typical Dataset Size**: 100-500 MB

## Advanced Usage

### Custom TIFF Processing

```python
import tifffile

# Custom TIFF reading with specific parameters
def load_custom_tiff(file_path):
    with tifffile.TiffFile(file_path) as tif:
        # Read with specific photometric interpretation
        volume = tif.asarray()
        
        # Extract metadata
        metadata = {}
        for page in tif.pages:
            for tag in page.tags:
                metadata[tag.name] = tag.value
                
        return volume, metadata
```

### Batch Processing

```python
# Process multiple EPFL datasets
datasets = [
    {
        "url": "https://example.com/dataset1.tif",
        "source_id": "EPFL-DATASET-001"
    },
    {
        "url": "https://example.com/dataset2.tif", 
        "source_id": "EPFL-DATASET-002"
    }
]

for dataset in datasets:
    config = EPFLConfig(
        download_url=dataset["url"],
        source_id=dataset["source_id"]
    )
    result = ingest_epfl_tif_with_config(config)
```

## Troubleshooting

### Common Issues

**Download Failures**
```bash
# Test URL accessibility
curl -I "https://documents.epfl.ch/groups/c/cv/cvlab-unit/www/data/%20ElectronMicroscopy_Hippocampus/volumedata.tif"
```

**TIFF Format Issues**
```python
# Validate TIFF file
import tifffile
try:
    with tifffile.TiffFile('dataset.tif') as tif:
        print(f"Pages: {len(tif.pages)}")
        print(f"Shape: {tif.pages[0].shape}")
        print(f"Dtype: {tif.pages[0].dtype}")
except Exception as e:
    print(f"TIFF validation failed: {e}")
```

**Memory Issues**
- Monitor available RAM before processing large files
- Use streaming for files >1GB
- Consider processing in chunks for very large datasets

**Slow Downloads**
- Increase `chunk_size_mb` for faster downloads
- Check network bandwidth and latency
- Use resume capability for interrupted downloads

### Debug Mode

```python
config = EPFLConfig(verbose=True, debug_mode=True)
# Enables detailed download progress and TIFF metadata logging
```

## Dataset Information

### Hippocampus Dataset Details

- **Volume Size**: 1024 × 1024 × 165 voxels
- **Physical Size**: 5.12 × 5.12 × 0.825 µm
- **Voxel Resolution**: 5nm isotropic
- **Data Type**: 8-bit grayscale
- **File Size**: ~173 MB
- **Acquisition Method**: Serial section EM

### Anatomical Context

- **Brain Region**: Hippocampus CA1
- **Species**: Rodent (likely mouse or rat)
- **Imaging Technique**: Serial section electron microscopy
- **Purpose**: Computer vision research and algorithm development

## Citation

When using EPFL CVLab data, please cite:

```
EPFL Computer Vision Laboratory (CVLab)
École Polytechnique Fédérale de Lausanne
https://www.epfl.ch/labs/cvlab/data/data-em/
```

## Dependencies

```
tifffile>=2023.1.0    # TIFF file reading
numpy>=1.21.0         # Array processing
requests>=2.28.0      # HTTP downloads
tqdm>=4.64.0          # Progress bars
```

## Related Documentation

- [EPFL CVLab](https://www.epfl.ch/labs/cvlab/)
- [EPFL EM Data](https://www.epfl.ch/labs/cvlab/data/data-em/)
- [TIFF Format Specification](https://www.adobe.io/open/standards/TIFF.html)
- [Tifffile Library](https://pypi.org/project/tifffile/)