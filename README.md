# electron-microscopy-ingest

A data pipeline for high-resolution electron microscopy images published by international institutions, generating high-dimensional vectors upstream of future ML & AI applications.

## Data Sources

The pipeline ingests from five major electron microscopy repositories:

| Source | Description | Format | Documentation |
|--------|-------------|--------|---------------|
| **[EBI EMPIAR](app/ebi/)** | Mouse synapse FIB-SEM (EMPIAR-11759) | DM3/MRC | [EBI Loader README](app/ebi/README.md) |
| **[EPFL CVLab](app/epfl/)** | Hippocampus TIFF stack | TIFF | [EPFL Loader README](app/epfl/README.md) |
| **[FlyEM/DVID](app/flyem/)** | Hemibrain connectome crops | Raw binary | [FlyEM Loader README](app/flyem/README.md) |
| **[IDR](app/idr/)** | Hippocampus volume (IDR0086) | OME-TIFF | [IDR Loader README](app/idr/README.md) |
| **[OpenOrganelle](app/openorganelle/)** | Mouse nucleus accumbens | Zarr/S3 | [OpenOrganelle Loader README](app/openorganelle/README.md) |

Each loader is containerized and runs in parallel using `docker compose`, with multithreading for efficient I/O operations.

## Prerequisites

- [Docker Compose](https://docs.docker.com/compose/install/) (for containerized execution)
- Python 3.8+ (for testing and development)
- [Conda](https://docs.conda.io/en/latest/miniconda.html) (optional, recommended for environment management)

## Execution

### Quick Start (Production Pipeline)

```bash
# Install dependencies
pip install -r requirements.txt

# Run all loaders in parallel
chmod +x scripts/run.sh
scripts/run.sh
```

### Development and Testing

```bash
# Optional: Create and activate conda environment
conda create -n em-ingest python=3.9
conda activate em-ingest

# Verify you're using the conda environment's Python and pip
which python  # Should show: /opt/miniconda3/envs/em-ingest/bin/python
which pip     # Should show: /opt/miniconda3/envs/em-ingest/bin/pip

# Install dependencies including testing tools
pip install -r requirements.txt

# If pip installs to system Python instead of conda env, use:
# conda deactivate && conda activate em-ingest
# /opt/miniconda3/envs/em-ingest/bin/pip install -r requirements.txt

# Run comprehensive test suite
python scripts/run_tests.py unit

# Test specific loader
python scripts/run_tests.py loader ebi

# Generate test report with coverage
python scripts/run_tests.py report
```

### Individual Loader Execution

```bash
# Run specific loader for development
docker compose up --build openorganelle

# Or run locally with custom configuration
cd app/ebi
python main.py
```

### Metadata Consolidation

```bash
# Enhanced consolidation with validation
cd app/consolidate
python main.py --config ../../config/config.yaml

# Legacy mode for backward compatibility
python main.py --legacy --root-dir ../..
```

### Execution Modes

The pipeline supports three execution modes via the `EM_EXECUTION_MODE` environment variable:

**Staged Execution** (Default - `EM_EXECUTION_MODE=staged`):
- Intelligent resource allocation with staged execution
- Optimized for constrained hardware (16GB memory, 2GHz CPU)
- Stage 1: Light services in parallel (IDR, FlyEM, EBI) - 3.5GB total
- Stage 2: Heavy services sequentially (OpenOrganelle → EPFL) - 9GB peak
- Memory efficiency: 67% better than parallel execution

**Parallel Execution** (`EM_EXECUTION_MODE=parallel`):
- All containers run simultaneously using `docker compose`
- Suitable for high-memory systems (32GB+)
- Peak usage: 15-20GB memory, 4+ CPU cores

**Sequential Execution** (`EM_EXECUTION_MODE=sequential`):
- Services run one at a time with full resource access
- Most memory-conservative but slowest approach
- Peak usage: ~9GB per service

The loaders use multithreading for efficient I/O operations, with configurable worker counts in `config/config.yaml`.

```bash
# Default staged execution (recommended for most systems)
scripts/run.sh

# Force parallel execution (for high-memory systems)
EM_EXECUTION_MODE=parallel scripts/run.sh

# Force sequential execution (most conservative)
EM_EXECUTION_MODE=sequential scripts/run.sh
```

## Metadata catalog & Data Governance

### Enhanced Metadata Management (v2.0)

The system now features a **modernized metadata management framework** with formal schema validation, standardized status tracking, and centralized configuration management. This foundation prepares the platform for future evolution into a full catalog API service with event-driven data pipelines.

**Key Improvements:**
- **JSON Schema Validation**: All metadata is validated against a formal schema (`schemas/metadata_schema.json`)
- **Standardized Status Tracking**: Consistent processing states across all data sources (`pending`, `processing`, `saving-data`, `complete`, `failed`, `cancelled`)
- **Centralized Configuration**: YAML-based configuration system (`config/config.yaml`) with environment variable support
- **Enhanced Consolidation Tool**: Rich validation reporting, data quality metrics, and processing summaries
- **Metadata Manager Library**: Programmatic interface for metadata operations with validation and status management

All ingestion pipelines produce metadata JSON files describing each imaging dataset. The metadata follows a **two-phase pattern**: first as a stub with initial information, then enriched with computed statistics after processing. This design ensures recoverability and provides visibility into ingestion state.

Each metadata record includes:
- **Core fields**: Universal metadata (description, volume shape, voxel size, etc.)
- **Technical fields**: File details, checksums, compression info
- **Provenance fields**: Data lineage, processing pipeline info, download URLs
- **Quality metrics**: Validation results, completeness scores
- **Status tracking**: Real-time processing state with timestamps and progress

For a concrete example of the distinct metadata keys across all data sources, see the [aggregated metadata catalog example](./app/consolidate/metadata_catalog_20250603_045601.json).

### Standardized Metadata Schema

All loaders generate metadata following a common JSON schema with these core fields:

- **Core**: `description`, `volume_shape`, `voxel_size_nm`, `data_type`, `modality`
- **Technical**: `file_size_bytes`, `sha256`, `compression`, `chunk_size`
- **Provenance**: `download_url`, `processing_pipeline`, `internal_zarr_path`
- **Status**: `pending`, `processing`, `saving-data`, `complete`, `failed`, `cancelled`

See [metadata schema](schemas/metadata_schema.json) for complete specification.


### Enhanced Consolidation Tool

The metadata consolidation tool provides comprehensive metadata aggregation, validation, and quality reporting. See the [Consolidation Tool README](app/consolidate/README.md) for detailed documentation.

**Quick Start:**
```bash
cd app/consolidate
python main.py --config ../../config/config.yaml
```

The tool generates three output files: enhanced catalog (JSON), validation report (JSON), and processing log.

## Monitoring:

When using `run.sh` (`docker compose`) logs are saved at the `logs/` subdirectory

## Development:

Building and running individual loaders locally (from the root of the repo) can be done like:
```
docker compose up --build openorganelle
```
Where the last string corresponds to the `docker-compose.yml`'s keys under `services:` (i.e. `ebi`, `epfl`, `flyem`, `idr`, `openorganelle`) for the service being developed.

## Configuration Management

The system now uses a centralized YAML configuration file (`config/config.yaml`) that supports:

- **Global Settings**: Processing parameters, logging, and resource limits
- **Source-Specific Configuration**: URLs, format support, and metadata mappings per data source
- **Environment Variables**: Dynamic configuration using `${VAR_NAME}` or `${VAR_NAME:default}` syntax
- **Docker Integration**: Resource limits and networking configuration
- **Development Mode**: Debug settings and testing options

**Key Configuration Sections:**
- `global`: System-wide settings (logging, processing, metadata management)
- `sources`: Per-source configuration (URLs, formats, processing parameters)
- `consolidation`: Metadata consolidation tool settings
- `docker`: Container resource limits and networking

## Roadmap: Evolution to Catalog API Service

### Current State (v2.0)
- ✅ JSON Schema validation and standardized metadata structure
- ✅ Centralized configuration management with YAML
- ✅ Enhanced consolidation tool with validation reporting
- ✅ Standardized status tracking across all data sources
- ✅ Programmatic metadata management library

### Next Phase (v3.0): API Service Architecture
The current file-based metadata system will evolve into a **modern catalog API service** with the following capabilities:

**Planned Architecture:**
- **REST/GraphQL API**: Programmatic access to metadata catalog
- **Event-Driven Pipelines**: Real-time metadata updates via Kafka/Redis streams
- **Database Backend**: PostgreSQL with JSONB for flexible metadata storage
- **Search & Discovery**: Elasticsearch integration for advanced querying
- **Data Lineage**: Graph-based tracking of dataset relationships and processing history
- **Policy Engine**: Automated data governance and compliance rules
- **Quality Gates**: Real-time data quality monitoring and alerting

**Migration Strategy:**
- **Phase 1**: Hybrid mode with both file-based and API access
- **Phase 2**: API-first with file system as cache layer  
- **Phase 3**: Full event-driven architecture with real-time processing

**Integration Points:**
- Data source scripts will transition from file-based metadata writing to API calls
- Event streaming for real-time pipeline monitoring and status updates
- Webhook integration for external system notifications
- OpenAPI/GraphQL schemas for standardized integration

This foundation ensures smooth migration to a state-of-the-art metadata catalog platform while maintaining backward compatibility and operational continuity.

## Development

Building and running individual loaders locally (from the root of the repo) can be done like:
```
docker compose up --build openorganelle
```
Where the last string corresponds to the `docker-compose.yml`'s keys under `services:` (i.e. `ebi`, `epfl`, `flyem`, `idr`, `openorganelle`) for the service being developed.

### Dependencies

Install Python dependencies:
```bash
pip install -r requirements.txt
```

Required packages:
- `jsonschema>=4.17.0`: JSON Schema validation
- `pyyaml>=6.0`: YAML configuration parsing
- `numpy>=1.21.0`: Array processing (existing dependency)
- Additional packages as defined in `requirements.txt`

## Testing Framework

### Comprehensive Test Suite (v2.0)

The system now includes a comprehensive testing framework with parameterized loaders, unit tests, integration tests, and performance benchmarks.

**Test Architecture:**
- **Parameterized Loaders**: All hardcoded URLs and IDs extracted into configurable parameters
- **Unit Tests**: Fast tests with mocked external dependencies
- **Integration Tests**: End-to-end testing with real APIs (network required)
- **Performance Tests**: Benchmarking and load testing
- **Test Configuration**: YAML-based test parameters and environment setup

### Running Tests

**Quick Start:**
```bash
# Run all unit tests (fast, no network required)
python scripts/run_tests.py unit

# Run tests for specific loader
python scripts/run_tests.py loader ebi

# Run integration tests (requires network)
python scripts/run_tests.py integration

# Generate comprehensive test report
python scripts/run_tests.py report
```

**Advanced Testing:**
```bash
# Fast unit tests with coverage
python scripts/run_tests.py unit --fast --coverage

# Test specific loader with verbose output
python scripts/run_tests.py loader flyem -vv

# Integration tests without network dependencies
python scripts/run_tests.py integration --no-network

# Performance benchmarking
python scripts/run_tests.py performance

# Test consolidation tool
python scripts/run_tests.py consolidation
```

### Testing with Real Data

The testing framework supports two modes: mock data (default) and real ingested data for comprehensive validation.

**Using Real Data for Testing:**

1. **First, run the ingestion pipeline to populate data:**
   ```bash
   # Run the full ingestion pipeline to download and process data
   scripts/run.sh
   
   # Check that data was ingested successfully
   ls -la ./data/*/
   ```

2. **Run tests against real data:**
   ```bash
   # Test with real ingested data (recommended for integration testing)
   scripts/run_tests.sh with-data
   
   # Or run integration tests manually (will use existing data if available)
   scripts/run_tests.sh integration
   
   # Run unit tests with mocked data (fast)
   scripts/run_tests.sh unit
   ```

**Data Directory Structure:**
After running `scripts/run.sh`, you should see populated data directories:
```
./data/
├── ebi/           # EMPIAR datasets with metadata
├── epfl/          # EPFL hippocampus data  
├── flyem/         # FlyEM hemibrain crops
├── idr/           # IDR OME-TIFF data
└── openorganelle/ # OpenOrganelle Zarr datasets
```

**Testing Modes:**

- **Mock Data Tests**: Fast unit tests using simulated data fixtures
- **Real Data Tests**: Integration tests using actual downloaded datasets
- **Hybrid Testing**: Automatically uses real data if available, falls back to mocks

**Troubleshooting Data Ingestion:**

```bash
# Check ingestion logs
ls -la ./logs/

# Verify configuration
cat ./config/config.yaml

# Test individual service
docker compose up --build ebi

# Check Docker container status  
docker compose ps
```

### Project Structure

```
scripts/
├── run.sh                   # Main pipeline execution script
├── run_lint.sh              # Code quality checking script
├── run_tests.py             # Python test runner with advanced options
└── run_tests.sh             # Bash test runner for quick testing

tests/
├── conftest.py              # Shared fixtures and configuration
├── test_unit_ebi.py         # EBI loader unit tests
├── test_unit_flyem.py       # FlyEM loader unit tests
├── test_integration.py      # Cross-loader integration tests
└── __init__.py

lib/
├── loader_config.py         # Parameterized loader configurations
├── metadata_manager.py      # Metadata validation and management
└── config_manager.py        # Centralized configuration

app/
├── consolidate/             # Metadata consolidation tool
├── ebi/                     # EBI EMPIAR loader
├── epfl/                    # EPFL CVLab loader
├── flyem/                   # FlyEM DVID loader
├── idr/                     # IDR loader
└── openorganelle/           # OpenOrganelle Zarr loader
```

### Test Configuration

Test parameters are configured in `config/config.yaml` under the `development.testing` section:

```yaml
development:
  testing:
    test_configs:
      ebi:
        entry_id: "11759"
        timeout_seconds: 300
        enable_downloads: false
      flyem:
        crop_size: [100, 100, 100]
        random_seed: 42
        enable_downloads: false
```

### Parameterized Loaders

All loaders now support configuration-driven operation:

**EBI Loader:**
- Configurable EMPIAR entry IDs, FTP servers, API endpoints
- Test mode with mocked downloads and smaller timeouts

**FlyEM Loader:**
- Configurable DVID servers, UUIDs, crop sizes
- Reproducible testing with fixed random seeds

**EPFL Loader:**
- Configurable download URLs and chunk sizes
- Mock mode for testing without large downloads

**IDR Loader:**
- Configurable image IDs and dataset mappings
- Test mode with sample datasets

**OpenOrganelle Loader:**
- Configurable S3 URIs and Zarr paths
- Anonymous S3 access for testing

## Code Quality

### Code Quality Tools

The project enforces code quality using automated linting and formatting tools:

**Tools in use:**
- **Pylint**: Comprehensive code analysis with custom configuration for scientific computing
- **Black**: Consistent code formatting (120 character line length)
- **isort**: Import statement organization and sorting
- **Flake8**: Style guide enforcement and error detection

### Running Code Quality Checks

**Local Development:**
```bash
# Run all code quality checks
scripts/run_lint.sh

# Individual tools
black --check .          # Check formatting
isort --check-only .     # Check imports
flake8 .                 # Style check
pylint lib/ app/         # Code analysis
```

**Auto-fixing Issues:**
```bash
# Fix formatting and imports
black .
isort .

# View detailed Pylint reports
cat pylint-output/*.txt
```

### Code Quality Configuration

**Configuration Files:**
- `.pylintrc`: Pylint configuration optimized for scientific computing
- `pyproject.toml`: Black, isort, and pytest configuration
- `.flake8`: Flake8 style checking rules

**CI/CD Integration:**
The GitHub Actions workflow includes a `code-quality` job that runs before all other stages and generates:
- Pylint scores and detailed reports
- Formatting and import compliance checks  
- Code quality artifacts and summaries

### CI/CD Integration

The test framework supports continuous integration:

```bash
# Generate JUnit XML for CI systems
python scripts/run_tests.py unit --junit-xml=test-results.xml

# Generate coverage reports
python scripts/run_tests.py unit --coverage

# Run fast tests only (for PR validation)
python scripts/run_tests.py unit --fast

# Full test suite with reporting
python scripts/run_tests.py report
```

### Test Markers

Tests are organized with pytest markers:

- `@pytest.mark.unit`: Fast unit tests with mocked dependencies
- `@pytest.mark.integration`: Tests requiring network access
- `@pytest.mark.slow`: Long-running tests (downloads, large datasets)
- `@pytest.mark.performance`: Benchmarking and performance tests

**Running specific test types:**
```bash
# Unit tests only
pytest -m unit

# Skip slow tests
pytest -m "not slow"

# Integration tests only
pytest -m integration
```