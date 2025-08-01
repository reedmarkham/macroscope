# EBI EMPIAR Loader

This loader ingests electron microscopy datasets from the EMBL-EBI EMPIAR (Electron Microscopy Public Image Archive) repository via FTP and REST API access.

## Overview

The EBI loader downloads and processes microscopy datasets from EMPIAR entries, supporting various file formats including DM3, DM4, MRC, REC, ST, and ALI files. It provides comprehensive metadata extraction and validation.

## Data Source

- **Repository**: EMBL-EBI EMPIAR (https://www.ebi.ac.uk/empiar/)
- **Default Dataset**: EMPIAR-11759 (Mouse synapse volume)
- **Access Method**: FTP download + REST API metadata
- **File Formats**: DM3, DM4, MRC, REC, ST, ALI

## Configuration

### Default Parameters

```python
entry_id = "11759"                              # EMPIAR entry ID
ftp_server = "ftp.ebi.ac.uk"                   # FTP server hostname  
api_base_url = "https://www.ebi.ac.uk/empiar/api/entry"  # API endpoint
output_dir = "empiar_volumes"                   # Local output directory
max_workers = 4                                 # Parallel download threads
```

### Configurable Parameters

The loader supports configuration via `lib.loader_config.EBIConfig`:

```python
from lib.loader_config import EBIConfig

config = EBIConfig(
    entry_id="12345",                    # Different EMPIAR entry
    output_dir="./data/ebi",            # Custom output directory
    max_workers=2,                       # Fewer threads
    timeout_seconds=1800,                # 30 minute timeout
    ftp_server="custom.ftp.server",      # Alternative FTP server
    verbose=True                         # Enable verbose logging
)
```

## Usage

### Basic Usage

```bash
cd app/ebi
python main.py
```

### Programmatic Usage

```python
from app.ebi.main import ingest_empiar
from lib.loader_config import EBIConfig

# Use default configuration
result = ingest_empiar("11759")

# Use custom configuration
config = EBIConfig(entry_id="12345", output_dir="./custom_output")
result = ingest_empiar_with_config(config)
```

### Docker Usage

```bash
# Build and run EBI loader
docker compose up --build ebi

# Run with custom entry ID
docker run -e EMPIAR_ENTRY_ID=12345 ebi-loader
```

## Processing Pipeline

1. **Metadata Fetching**: Query EMPIAR API for entry metadata
2. **File Discovery**: List available files via FTP
3. **File Download**: Download data files with progress tracking
4. **Format Detection**: Identify file format and select appropriate loader
5. **Data Processing**: Load and convert to standardized numpy arrays
6. **Metadata Generation**: Create v2.0 schema-compliant metadata using MetadataManager
7. **Validation**: Automatic validation against JSON schema during metadata creation
8. **Storage**: Save processed volumes and validated metadata

## Output Structure

```
data/ebi/
├── downloads/                                              # Raw downloaded files
│   ├── F57-8_test1_3VBSED_slice_0002.dm3
│   └── F57-8_test1_3VBSED_slice_0003.dm3
├── F57-8_test1_3VBSED_slice_0002_20250607_163832.npy     # Processed volume
└── F57-8_test1_3VBSED_slice_0002_20250607_163832_metadata.json  # v2.0 metadata record
```

## Metadata Schema (v2.0)

The loader generates metadata following the v2.0 standardized schema using the `MetadataManager` library for schema compliance:

```json
{
  "id": "c5239419-2241-4651-8771-032b02e07f7d",
  "source": "ebi",
  "source_id": "11759",
  "status": "complete",
  "created_at": "2025-06-07T16:38:32.886969+00:00",
  "updated_at": "2025-06-07T16:38:33.095014+00:00",
  "metadata": {
    "core": {
      "description": "EBI EMPIAR dataset 11759",
      "volume_shape": [5500, 5496],
      "data_type": "uint8"
    },
    "technical": {
      "file_size_bytes": 30228000,
      "sha256": "56f7bf65fdbca747b6feefc6822a8cfb1de73d9f41a4f19581619bcf1000a292"
    },
    "provenance": {
      "download_url": "ftp://ftp.ebi.ac.uk/empiar/world_availability/11759/data/F57-8_test1_3VBSED_slice_0002.dm3"
    }
  },
  "files": {
    "metadata": "/app/data/ebi/F57-8_test1_3VBSED_slice_0002_20250607_163832_metadata.json",
    "raw": "/app/data/ebi/downloads/F57-8_test1_3VBSED_slice_0002.dm3",
    "volume": "/app/data/ebi/F57-8_test1_3VBSED_slice_0002_20250607_163832.npy"
  },
  "additional_metadata": {
    "EMPIAR-11759": {
      "title": "Developing retina in zebrafish 55 hpf larval eye.",
      "authors": [{"author": {"name": "Wilsch-Bräuninger M"}}],
      "deposition_date": "2023-11-01",
      "release_date": "2024-01-15"
    }
  }
}
```

**Key v2.0 Features:**
- **Schema Compliance**: Validated against `schemas/metadata_schema.json`
- **MetadataManager Integration**: Uses standardized library for metadata generation
- **UUID Generation**: Unique identifier for each record
- **Timestamp Tracking**: Created and updated timestamps in ISO format
- **File Path Tracking**: Complete file location mapping
- **Enhanced EMPIAR Metadata**: Rich metadata from EMPIAR API integrated as additional_metadata

## Supported File Formats

### DM3/DM4 (Digital Micrograph)
- Uses `ncempy` library for reading
- Supports metadata extraction
- Handles multiple datasets per file

### MRC (Medical Research Council)
- Uses `mrcfile` library
- Standard electron microscopy format
- Preserves voxel size information

### REC/ST/ALI (Tomography formats)
- Processed as MRC-compatible
- Common in electron tomography

## Error Handling

The loader implements comprehensive error handling:

- **Network Errors**: Retry logic for FTP and API failures
- **File Format Errors**: Graceful degradation with format detection
- **Memory Errors**: Chunked processing for large files
- **Validation Errors**: Detailed error reporting

## Testing

### Unit Tests

```bash
# Run EBI-specific unit tests
python run_tests.py loader ebi

# Run with mocked dependencies
python run_tests.py unit --loader ebi
```

### Integration Tests

```bash
# Test with real API (requires network)
python run_tests.py integration --loader ebi

# Test with small dataset
pytest tests/test_unit_ebi.py::TestEBIIntegration::test_small_file_download
```

### Test Configuration

```yaml
development:
  testing:
    test_configs:
      ebi:
        entry_id: "11759"
        timeout_seconds: 300
        max_workers: 2
        enable_downloads: false  # Mock downloads in tests
```

## Performance Characteristics

- **Typical Download Speed**: 10-50 MB/s (network dependent)
- **Processing Speed**: ~100 MB/s for format conversion
- **Memory Usage**: ~2x final volume size during processing
- **Parallel Downloads**: Up to 4 concurrent files

## Troubleshooting

### Common Issues

**FTP Connection Failures**
```bash
# Check FTP connectivity
ftp ftp.ebi.ac.uk
# cd /empiar/world_availability/11759/data
```

**API Timeouts**
```bash
# Test API accessibility
curl https://www.ebi.ac.uk/empiar/api/entry/11759/
```

**Format Reading Errors**
- Ensure required libraries are installed: `ncempy`, `mrcfile`
- Check file integrity with alternative tools
- Verify file format matches extension

**Memory Issues**
- Reduce `max_workers` for large files
- Use streaming processing for very large datasets
- Monitor available disk space

### Debug Mode

```python
config = EBIConfig(verbose=True, debug_mode=True)
# Enables detailed logging and intermediate file preservation
```

## Dependencies

```
ncempy>=1.8.0      # DM3/DM4 file reading
mrcfile>=1.3.0     # MRC file reading  
numpy>=1.21.0      # Array processing
requests>=2.28.0   # HTTP API calls
tqdm>=4.64.0       # Progress bars
```

## Related Documentation

- [EMPIAR Database](https://www.ebi.ac.uk/empiar/)
- [EMPIAR API Documentation](https://www.ebi.ac.uk/empiar/api/entry/)
- [Digital Micrograph Format](https://www.gatan.com/products/tem-analysis/gatan-microscopy-suite-software)
- [MRC Format Specification](https://www.ccpem.ac.uk/mrc_format/mrc2014.php)