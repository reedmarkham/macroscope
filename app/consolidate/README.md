# Metadata Consolidation Tool

This tool provides comprehensive metadata aggregation, validation, and quality reporting across all electron microscopy data sources in the pipeline.

## Overview

The consolidation tool scans output directories from all loaders, validates metadata against the JSON schema, and generates detailed reports on data quality, completeness, and processing status. It serves as the central quality control and catalog generation component.

## Features

### Enhanced Capabilities (v2.0)
- **Schema Validation**: Validates all metadata against formal JSON schema using MetadataManager
- **Quality Assessment**: Generates completeness scores and error reports
- **Configuration-Driven**: Uses centralized YAML configuration for flexibility
- **Multiple Output Formats**: JSON catalogs, validation reports, and processing logs
- **Backward Compatibility**: Maintains legacy API while adding new capabilities
- **Real-time Processing**: Live validation and error reporting during consolidation
- **Legacy Migration**: Automatic detection and conversion of old metadata formats to v2.0 schema

## Configuration

### Default Behavior
The tool automatically scans standard output directories:
- `./data/ebi/` - EBI EMPIAR volumes
- `./data/epfl/` - EPFL CVLab data
- `./data/flyem/` - FlyEM DVID crops
- `./data/idr/` - IDR volumes
- `./data/openorganelle/` - OpenOrganelle Zarr volumes

### Configuration File
Behavior is controlled via `config/config.yaml`:

```yaml
consolidation:
  enabled: true
  output_dir: "./app/consolidate"
  
  processing:
    scan_directories: 
      - "./data/ebi"
      - "./data/epfl" 
      - "./data/flyem"
      - "./data/idr"
      - "./data/openorganelle"
    
    include_patterns: ["*metadata*.json"]
    exclude_patterns: ["*.tmp", "*.log", "*.backup"]
    timestamp_format: "%Y%m%d_%H%M%S"
  
  validation:
    strict_mode: false
    report_validation_errors: true
    fail_on_invalid: false
  
  quality:
    check_completeness: true
    detect_duplicates: true
    generate_statistics: true
    min_required_fields: ["source", "source_id", "description", "status"]
```

## Usage

### Enhanced Mode (Recommended)

```bash
cd app/consolidate
python main.py --config ../../config/config.yaml
```

**Command Line Options:**
- `--config PATH`: Path to configuration file
- `--legacy`: Use legacy consolidation mode
- `--root-dir PATH`: Root directory for legacy mode

### Legacy Mode (Backward Compatible)

```bash
cd app/consolidate
python main.py --legacy --root-dir ../..
```

### Docker Mode

```bash
cd app/consolidate
docker build -t metadata-consolidator .
docker run --rm \
  -v "$PWD/../..:/repo" \
  -w /repo/app/consolidate \
  metadata-consolidator
```

### Programmatic Usage

```python
from app.consolidate.main import consolidate_metadata_with_validation
from lib.config_manager import ConfigManager

# Use default configuration
consolidate_metadata_with_validation()

# Use custom configuration
config_manager = ConfigManager("custom_config.yaml")
consolidate_metadata_with_validation("custom_config.yaml")
```

## Output Files

The tool generates three types of output files:

### 1. Enhanced Catalog (`metadata_catalog_TIMESTAMP.json`)
Comprehensive catalog with validation statistics and quality metrics:

```json
{
  "metadata": {
    "generated_at": "2024-01-01T12:00:00Z",
    "schema_version": "1.0",
    "consolidation_version": "2.0-enhanced"
  },
  "summary": {
    "total_files": 25,
    "processed_files": 25,
    "validation_errors": 2,
    "sources_found": ["ebi", "epfl", "flyem", "idr", "openorganelle"]
  },
  "validation": {
    "total_files": 25,
    "valid_files": 23,
    "invalid_files": 2,
    "validation_rate": 92.0,
    "errors_by_source": {
      "ebi": {"valid": 5, "invalid": 0},
      "flyem": {"valid": 3, "invalid": 2}
    }
  },
  "legacy_compatibility": {
    "distinct_keys": ["source", "source_id", "description", ...],
    "key_ratios_by_source": {...},
    "total_files_by_source": {...}
  },
  "data_quality": {
    "completeness_by_source": {
      "ebi": {"average_fields": 15.2, "required_fields_present": 100.0}
    },
    "schema_compliance_rate": 92.0
  }
}
```

### 2. Validation Report (`validation_report_TIMESTAMP.json`)
Detailed validation results for each metadata file:

```json
{
  "generated_at": "2024-01-01T12:00:00Z",
  "summary": {
    "total_files": 25,
    "valid_files": 23,
    "invalid_files": 2,
    "validation_rate": 92.0
  },
  "detailed_results": [
    {
      "file": "/data/ebi/metadata_EMPIAR-11759_20240101.json",
      "source": "ebi",
      "valid": true,
      "errors": []
    },
    {
      "file": "/data/flyem/metadata_crop_12000_15000_8000.json",
      "source": "flyem",
      "valid": false,
      "errors": ["Missing required field: voxel_size_nm"]
    }
  ]
}
```

### 3. Processing Log (`metadata_catalog_TIMESTAMP.log`)
Detailed processing log with file-by-file status:

```
📂 Found 25 candidate metadata files.

🚀 Starting enhanced metadata consolidation with validation
📊 Schema validation: reporting only
📁 Scanning directories: ['./data/ebi', './data/epfl', ...]

📄 Processed metadata_EMPIAR-11759_20240101.json (source=ebi, 12 keys)
✅ Valid metadata: metadata_EMPIAR-11759_20240101.json (ebi)
⚠️ Validation failed for metadata_crop_xyz.json (flyem): 1 errors

📊 CONSOLIDATION SUMMARY
🔑 Found 18 distinct metadata keys across 5 sources
📄 Processed 25/25 files
✅ Schema validation: 23 valid, 2 invalid (92.0% valid)
📁 Sources found: ebi, epfl, flyem, idr, openorganelle
```

## Validation Engine

### Schema Validation
All metadata files are validated against the JSON schema at `schemas/metadata_schema.json`:

- **Required Fields**: `id`, `source`, `source_id`, `status`, `created_at`, `metadata`
- **Field Types**: String, number, array, object validation
- **Format Validation**: UUID, datetime, URL format checking
- **Enum Validation**: Status values, source names, data types
- **Legacy Detection**: Automatic identification of old metadata formats requiring migration
- **MetadataManager Integration**: Uses standardized validation library for consistency

### Data Quality Metrics

**Completeness Score**: Percentage of expected fields present
```python
completeness = (present_fields / expected_fields) * 100
```

**Validation Rate**: Percentage of files passing schema validation
```python
validation_rate = (valid_files / total_files) * 100
```

**Error Classification**: Categorization of validation errors by type and frequency

## Quality Assessment

### Completeness Analysis
- **Field Presence**: Which metadata fields are present across sources
- **Required Fields**: Compliance with minimum required field set
- **Optional Fields**: Adoption rate of optional metadata fields

### Cross-Source Consistency
- **Schema Compliance**: Validation against common schema
- **Field Naming**: Consistency of field names and types
- **Value Formats**: Standardization of value formats (dates, UUIDs, etc.)

### Error Detection
- **Missing Fields**: Required fields not present
- **Type Mismatches**: Incorrect data types
- **Format Violations**: Invalid formats (UUIDs, dates, URLs)
- **Enum Violations**: Invalid enumerated values

## Advanced Features

### Strict Mode
Enable strict validation that fails on any validation errors:

```yaml
consolidation:
  validation:
    strict_mode: true
    fail_on_invalid: true
```

When enabled:
- Process exits with non-zero code if validation errors occur
- Suitable for CI/CD pipelines requiring perfect metadata quality
- Provides immediate feedback on metadata issues

### Custom Quality Rules
Define custom quality checks in configuration:

```yaml
consolidation:
  quality:
    min_required_fields: ["source", "source_id", "description", "status"]
    custom_rules:
      - field: "voxel_size_nm"
        required_for_sources: ["ebi", "idr", "openorganelle"]
      - field: "sha256"
        format: "^[a-f0-9]{64}$"
```

### Duplicate Detection
Automatically detect duplicate datasets based on:
- Source ID matching across different processing runs
- SHA256 hash matching for identical volumes
- Similar metadata patterns indicating reprocessing

## Error Handling

### File Processing Errors
- **JSON Parse Errors**: Invalid JSON syntax
- **File Access Errors**: Permission or file system issues
- **Schema Loading Errors**: Problems with schema file

### Validation Errors
- **Missing Required Fields**: Required metadata fields not present
- **Type Validation**: Incorrect data types for fields
- **Format Validation**: Invalid formats for structured fields
- **Enum Validation**: Invalid values for controlled vocabularies

### Recovery Strategies
- **Graceful Degradation**: Continue processing despite individual file failures
- **Error Reporting**: Comprehensive error details for debugging
- **Partial Success**: Generate reports for successfully processed files

## Performance Characteristics

- **Processing Speed**: ~100-500 files/second depending on validation complexity
- **Memory Usage**: ~10MB baseline + ~1KB per metadata file
- **Disk I/O**: Optimized with batch file reading and atomic writes
- **Scalability**: Handles thousands of metadata files efficiently

## Integration

### Production Pipeline Integration
```bash
# Validate all metadata in production
python main.py --config production.yaml
if [ $? -ne 0 ]; then
  echo "Metadata validation failed!"
  exit 1
fi
```

### Automated Quality Monitoring
```python
from app.consolidate.main import consolidate_metadata_with_validation
import json

# Run consolidation and check quality
consolidate_metadata_with_validation()

# Parse results for monitoring
with open('validation_report_latest.json') as f:
    report = json.load(f)
    
validation_rate = report['summary']['validation_rate']
if validation_rate < 95.0:
    send_alert(f"Metadata quality below threshold: {validation_rate}%")
```

### API Integration (Future)
The consolidation tool is designed for future integration with metadata catalog APIs:

```python
# Future API integration
async def sync_to_catalog_api():
    catalog_data = load_consolidated_metadata()
    async with aiohttp.ClientSession() as session:
        await session.post('/api/metadata/bulk', json=catalog_data)
```

## Testing

### Unit Tests
```bash
# Test consolidation logic
python run_tests.py consolidation

# Test with mock data
pytest tests/test_consolidation.py::test_validation_pipeline
```

### Integration Tests
```bash
# Test with real metadata files
python run_tests.py integration --loader consolidation

# Test configuration loading
pytest tests/test_integration.py -k "consolidation"
```

## Troubleshooting

### Common Issues

**No Metadata Files Found**
- Verify scan directories exist and contain metadata files
- Check include/exclude patterns in configuration
- Ensure metadata files follow naming convention

**Schema Validation Failures**
- Review JSON schema at `schemas/metadata_schema.json`
- Check field names and types in failing metadata files
- Use legacy migration script (`scripts/fix_legacy_metadata.py`) for old formats
- Validate JSON syntax with external tools

**Configuration Errors**
- Verify YAML syntax in configuration file
- Check file paths are correct and accessible
- Ensure all required configuration sections are present

**Performance Issues**
- Reduce number of scan directories for faster processing
- Disable detailed validation for large metadata collections
- Use SSD storage for metadata files when possible

### Debug Mode
```bash
# Enable verbose logging
python main.py --config config.yaml -vv

# Use development configuration with debug settings
python main.py --config examples/config_development.yaml
```

## Related Documentation

- [Main Pipeline README](../../README.md) - Overview of the entire pipeline
- [Metadata Schema](../../schemas/metadata_schema.json) - JSON schema specification
- [Configuration Guide](../../config/config.yaml) - Configuration file reference
- [Testing Guide](../../README.md#testing-framework) - Testing framework documentation