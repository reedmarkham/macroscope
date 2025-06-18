# macroscope: a data pipeline for high-resolution electron microscopy images

## Data Sources

The pipeline ingests from five major electron microscopy repositories:

| Source | Description | Format | Documentation |
|--------|-------------|--------|---------------|
| **[EBI EMPIAR](app/ebi/)** | Mouse synapse FIB-SEM (EMPIAR-11759) | DM3/MRC | [EBI Loader README](app/ebi/README.md) |
| **[EPFL CVLab](app/epfl/)** | Hippocampus TIFF stack | TIFF | [EPFL Loader README](app/epfl/README.md) |
| **[DVID FlyEM](app/flyem/)** | Hemibrain connectome crops | Raw binary | [FlyEM Loader README](app/flyem/README.md) |
| **[IDR](app/idr/)** | Hippocampus volume (IDR0086) | OME-TIFF | [IDR Loader README](app/idr/README.md) |
| **[OpenOrganelle](app/openorganelle/)** | Mouse nucleus accumbens | Zarr/S3 | [OpenOrganelle Loader README](app/openorganelle/README.md) |

Each loader is containerized and runs in parallel using `docker compose`, with multithreading for efficient I/O operations.

The pipeline also introduces a containerized metadata app using JSON schema validation (see [Metadata Consolidation](#metadata-consolidation)).

## Prerequisites

- [Docker Compose](https://docs.docker.com/compose/install/) (for containerized execution)
- Python 3.8+ (for testing and development)
- [Conda](https://docs.conda.io/en/latest/miniconda.html) (optional, recommended for environment management)

## Execution

### Quick Start (Local Installation)

Open Docker Desktop.

Activate your conda environment:
```bash
conda activate em-ingest
```

Install dependencies:
```bash
pip install -r requirements.txt
```

**Optional**: Configure large array processing (see [Configuration Management](#configuration-management)):
```bash
# Copy example configuration and customize
cp .env.example .env              # Create .env from template
cat .env                          # View current settings
# Edit .env file to modify settings as needed
```

Execute orchestrated workflow of all 5 data loaders:
```bash
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
# If `which pip` did not show the miniconda3 executable above, then:
/opt/miniconda3/envs/em-ingest/bin/pip install -r requirements.txt

# Run comprehensive test suite
python scripts/run_tests.py unit

# Test specific loader
python scripts/run_tests.py loader ebi

# Generate test report with coverage
python scripts/run_tests.py report
```

#### Individual Loader Execution

```bash
# Run specific loader for development
docker compose up --build openorganelle

# Or run locally with custom configuration
cd app/ebi
python main.py
```

### Metadata Consolidation

The metadata consolidation service runs **automatically** after all data loaders complete. Results are saved to the `metadata/` directory:

```bash
# Consolidation runs automatically with the pipeline
scripts/run.sh

# Manual consolidation (if needed)
docker compose run --rm consolidate

# Or run locally for development
cd app/consolidate
python main.py --config ../../config/config.yaml
```

**Output Location**: All consolidated metadata catalogs, validation reports, and processing logs are written to the `metadata/` directory for centralized access.

### Execution Modes

The pipeline supports four execution modes via the `EM_EXECUTION_MODE` environment variable:

**Staged Execution** (Default - `EM_EXECUTION_MODE=staged`):
- Intelligent resource allocation with staged execution
- Optimized for constrained hardware (16GB memory, 2GHz CPU)
- Stage 1: Light services in parallel (IDR, FlyEM, EBI, EPFL) - 13.5GB total
- Stage 2: Heavy service (OpenOrganelle) with dedicated bandwidth - 2GB peak
- Memory efficiency: Optimized for concurrent processing with bandwidth isolation

**Background Orchestrated Execution** (`EM_EXECUTION_MODE=background`):
- **Optimized throughput with intelligent overlapping** 🚀
- All small loaders run in parallel: IDR + FlyEM + EBI + EPFL (13.5GB total)
- OpenOrganelle starts after all small loaders complete (2GB, isolated bandwidth)
- **Peak usage: 13.5GB memory, ~15-20 minutes total time**
- **Best balance of efficiency and memory safety**

**Parallel Execution** (`EM_EXECUTION_MODE=parallel`):
- All containers run simultaneously using `docker compose`
- Suitable for high-memory systems (32GB+)
- Peak usage: 15.5GB memory, 4+ CPU cores

**Sequential Execution** (`EM_EXECUTION_MODE=sequential`):
- Services run one at a time with full resource access
- Most memory-conservative but slowest approach
- Peak usage: ~10GB per service

The loaders use multithreading for efficient I/O operations, with configurable worker counts in `config/config.yaml`.

### OpenOrganelle Performance Optimization

The OpenOrganelle loader features **adaptive chunking optimization** to handle large Zarr arrays efficiently:

**Adaptive Processing Strategy:**
- **Small arrays (<50MB)**: Direct computation for maximum speed
- **Medium arrays (50-500MB)**: Balanced chunking for optimal memory/performance trade-off  
- **Large arrays (≥500MB)**: **I/O-optimized chunking** with enhanced parallelism

**Memory Management:**
- **Improved memory estimation**: Accurate sizing prevents 0.0MB estimation errors
- **Chunk explosion prevention**: Caps at 10,000 chunks to avoid overhead
- **Progressive feedback**: Real-time progress monitoring for large arrays

**I/O Optimization & CPU Utilization:**
- **Aggressive CPU utilization**: Intelligent orchestration targeting 75-85% CPU usage
- **Dynamic concurrent processing**: Up to 6 arrays processed simultaneously with memory-aware throttling
- **Real-time resource monitoring**: CPU and memory tracking with adaptive scheduling
- **Enhanced orchestration**: 3-phase submission (small→medium→large) with deferred array queueing
- **Parallel I/O threads**: 12 threads for S3 network operations (tripled from 4)
- **Optimized Dask configuration**: Disabled fusion for better I/O throughput

**Performance Monitoring:**
- Processing rates (MB/s) and throughput metrics
- Memory usage tracking during computation
- **CPU utilization tracking**: Monitors I/O vs CPU bottlenecks

**Tuning Parameters:**
```bash
# Reduce chunk size for memory-constrained systems  
export ZARR_CHUNK_SIZE_MB=32

# Increase workers for aggressive CPU utilization (automatically triples I/O threads)
export MAX_WORKERS=8

# Increase memory and CPU allocation for large arrays
export MEMORY_LIMIT_GB=12
export OPENORGANELLE_CPU_LIMIT=6.0  # Increased to 4.0 (default), up to 6.0 for high-CPU systems
```

**Performance Improvements:**
- **Enhanced CPU utilization**: Targeting 75-85% CPU usage with intelligent orchestration
- **Concurrent processing**: Up to 6 arrays processed simultaneously vs. previous sequential approach
- **Dynamic resource management**: Real-time memory and CPU monitoring with adaptive scheduling
- **Processing time**: Reduced from 45+ minutes to ~10-15 minutes for GB-scale datasets
- **I/O throughput**: Enhanced parallel S3 operations with 12 concurrent threads

```bash
# Default staged execution (recommended for most systems)
scripts/run.sh

# Background orchestrated execution (recommended for optimal throughput)
EM_EXECUTION_MODE=background scripts/run.sh

# Force parallel execution (for high-memory systems)
EM_EXECUTION_MODE=parallel scripts/run.sh

# Force sequential execution (most conservative)
EM_EXECUTION_MODE=sequential scripts/run.sh
```

**Execution Mode Comparison:**

| Mode | Memory Peak | Time Est. | Best For |
|------|-------------|-----------|----------|
| `background` | 13.5GB | 15-20 min | **Optimal throughput + memory safety** ⭐ |
| `staged` | 13.5GB | 18-23 min | Parallel processing with bandwidth isolation |
| `sequential` | 10GB | 35-40 min | Maximum memory conservation |
| `parallel` | 15.5GB | 15-20 min | High-memory systems (32GB+) |

## Metadata catalog & Data Governance

### Enhanced Metadata Management (v2.0)

The system features a **robust metadata management framework** with formal schema validation, standardized status tracking, and centralized configuration management. All loaders have been updated to use the new MetadataManager library for consistent, schema-compliant metadata generation.

**Key Features:**
- **JSON Schema Validation**: All metadata is validated against a formal schema (`schemas/metadata_schema.json`)
- **Standardized Status Tracking**: Consistent processing states across all data sources (`pending`, `processing`, `saving-data`, `complete`, `failed`, `cancelled`)
- **Centralized Configuration**: YAML-based configuration system (`config/config.yaml`) with environment variable support
- **Enhanced Consolidation Tool**: Rich validation reporting, data quality metrics, and processing summaries
- **Metadata Manager Library**: Programmatic interface for metadata operations with validation and status management
- **Schema Compliance**: All loaders generate v2.0 schema-compliant metadata with proper field structure and validation

All ingestion pipelines produce metadata JSON files describing each imaging dataset. The metadata follows a **two-phase pattern**: first as a stub with initial information, then enriched with computed statistics after processing. This design ensures recoverability and provides visibility into ingestion state.

Each metadata record includes:
- **Core fields**: Universal metadata (description, volume shape, voxel size, etc.)
- **Technical fields**: File details, checksums, compression info
- **Provenance fields**: Data lineage, processing pipeline info, download URLs
- **Quality metrics**: Validation results, completeness scores
- **Status tracking**: Real-time processing state with timestamps and progress

For a concrete example of the distinct metadata keys across all data sources, see the aggregated metadata catalog generated in the `metadata/` directory after running the pipeline.

### Standardized Metadata Schema

All loaders generate metadata following a common JSON schema with these core fields:

- **Core**: `description`, `volume_shape`, `voxel_size_nm`, `data_type`, `modality`
- **Technical**: `file_size_bytes`, `sha256`, `compression`, `chunk_size`
- **Provenance**: `download_url`, `processing_pipeline`, `internal_zarr_path`
- **Status**: `pending`, `processing`, `saving-data`, `complete`, `failed`, `cancelled`

See [metadata schema](schemas/metadata_schema.json) for complete specification.

#### Schema Validation & Compliance

The pipeline includes **automatic schema validation** to ensure metadata consistency across all data sources:

- **JSON Schema**: All metadata files are validated against `schemas/metadata_schema.json`
- **Validation Reports**: Detailed validation results in `metadata/validation_report_*.json`
- **Quality Metrics**: Schema compliance scores and error summaries
- **Standards Compliance**: Ensures all loaders produce consistent, interoperable metadata
- **Legacy Migration**: Automatic conversion of old metadata formats to v2.0 schema structure

**Recent Improvements:**
- All loaders now use the unified `MetadataManager` library for schema-compliant metadata generation
- Standardized field structure with required `id`, `created_at`, `status`, and nested `metadata` sections
- Automated validation during metadata creation with detailed error reporting
- Fixed source field standardization (EMPIAR entries now correctly use "ebi" source)

**Example validation output:**
```json
{
  "validation_summary": {
    "total_files": 102,
    "valid_files": 102,
    "invalid_files": 0,
    "validation_rate": 100.0
  },
  "schema_compliance": {
    "required_fields_present": 100.0,
    "data_type_compliance": 100.0,
    "format_compliance": 100.0
  }
}
```

**Legacy Metadata Handling:**
If you encounter validation failures due to old metadata files, you have several options:

1. **Regenerate fresh metadata**: Re-run the pipeline to create new schema-compliant metadata
2. **Use migration tools**: The system includes migration scripts (`scripts/fix_legacy_metadata.py`) to convert old formats
3. **Manual cleanup**: Remove old metadata files and let the pipeline regenerate them

```bash
# Regenerate all metadata (recommended)
scripts/run.sh

# Check validation status
docker compose run --rm consolidate
```


### Enhanced Consolidation Tool

The metadata consolidation tool provides comprehensive metadata aggregation, validation, and quality reporting. It runs **automatically** after all data loaders complete and outputs results to the `metadata/` directory.

**Automatic Execution:**
```bash
# Consolidation runs automatically with the main pipeline
scripts/run.sh
```

**Manual Execution:**
```bash
# Run consolidation independently
docker compose run --rm consolidate

# Or run locally for development
cd app/consolidate
python main.py --config ../../config/config.yaml
```

**Output Files** (in `metadata/` directory):
- `metadata_catalog_YYYYMMDD_HHMMSS.json`: Enhanced catalog with validation results
- `validation_report_YYYYMMDD_HHMMSS.json`: Detailed validation and quality metrics  
- `metadata_catalog_YYYYMMDD_HHMMSS.log`: Processing log with detailed output

See the [Consolidation Tool README](app/consolidate/README.md) for detailed documentation.

### Enhanced Logging & Orchestration

The pipeline features **improved logging and orchestration** with clear distinctions between system-level and service-level activities:

**Pipeline Orchestration Logging:**
- **Clear service orchestration messaging**: Distinguished between Docker container orchestration and individual service activities
- **Structured log prefixes**: `SUCCESS:`, `FAILED:`, `CONFIG:`, `PROCESSING:` for easier log parsing
- **Clean professional output**: Removed emoji usage for cleaner, more professional logs
- **Orchestration clarity**: Enhanced messaging to distinguish between "orchestrating containers" vs "service processing"

**Service-Level Logging:**
- **Individual service logs**: Each loader produces detailed logs in the `logs/` directory
- **Technical progress tracking**: Processing rates, memory usage, and performance metrics
- **Error context**: Detailed error reporting with stack traces and troubleshooting hints
- **Status tracking**: Real-time processing state updates with timestamps

**Log Organization:**
```bash
# Pipeline orchestration logs (run.sh script activities)
logs/stage1_YYYYMMDD_HHMMSS.log     # Light services orchestration
logs/stage2_YYYYMMDD_HHMMSS.log     # Heavy services orchestration

# Individual service logs (container outputs)
logs/ebi_YYYYMMDD_HHMMSS.log        # EBI loader processing details
logs/openorganelle_YYYYMMDD_HHMMSS.log  # OpenOrganelle processing details
```

## Output Structure

The pipeline generates organized outputs in two main directories:

### Data Directory (`data/`)
Raw ingested data and individual metadata files:
```
data/
├── ebi/           # EMPIAR datasets with metadata
├── epfl/          # EPFL hippocampus data  
├── flyem/         # FlyEM hemibrain crops
├── idr/           # IDR OME-TIFF data
└── openorganelle/ # OpenOrganelle Zarr datasets
```

### Metadata Directory (`metadata/`)
Consolidated metadata catalogs and validation reports:
```
metadata/
├── metadata_catalog_YYYYMMDD_HHMMSS.json    # Comprehensive metadata catalog
├── validation_report_YYYYMMDD_HHMMSS.json   # Schema validation results
└── metadata_catalog_YYYYMMDD_HHMMSS.log     # Processing logs
```

### Logs Directory (`logs/`)
Execution logs from pipeline runs:
```
logs/
├── stage1_YYYYMMDD_HHMMSS.log     # Stage 1 loader execution logs (EBI, EPFL, FlyEM, IDR)
├── stage2_YYYYMMDD_HHMMSS.log     # Stage 2 loader execution logs (OpenOrganelle)
└── consolidate_YYYYMMDD_HHMMSS.log # Consolidation service logs
```

## Development:

Building and running individual loaders locally (from the root of the repo) can be done like:
```
docker compose up --build openorganelle
```
Where the last string corresponds to the `docker-compose.yml`'s keys under `services:` (i.e. `ebi`, `epfl`, `flyem`, `idr`, `openorganelle`) for the service being developed.

## Configuration Management

The system uses a **multi-layer configuration approach** with three sources in order of priority:

### 1. Environment Configuration (`.env` file) - **Highest Priority**

The `.env` file provides centralized configuration for both Docker Compose and the application:

**Setup:**
```bash
# Copy the example configuration template
cp .env.example .env

# Edit the configuration as needed
nano .env  # or your preferred editor
```

```bash
# Large array processing mode: "skip", "stream", or "downsample"
LARGE_ARRAY_MODE=stream

# Memory and performance settings
MAX_ARRAY_SIZE_MB=500
STREAMING_CHUNK_MB=2
DOWNSAMPLE_FACTOR=4
MAX_WORKERS=1
ZARR_CHUNK_SIZE_MB=8
MEMORY_LIMIT_GB=2

# Container resource limits
OPENORGANELLE_MEMORY_LIMIT=2g
OPENORGANELLE_CPU_LIMIT=0.5
```

**Usage:**
```bash
# Modify .env file to change settings
echo "LARGE_ARRAY_MODE=downsample" > .env

# Restart containers to apply changes
docker compose up --build openorganelle
```

### 2. YAML Configuration (`config/config.yaml`) - **Fallback**

The centralized YAML configuration file provides defaults when environment variables aren't set:

- **Global Settings**: Processing parameters, logging, and resource limits
- **Source-Specific Configuration**: URLs, format support, and metadata mappings per data source
- **Docker Integration**: Resource limits and networking configuration
- **Development Mode**: Debug settings and testing options

**Key Configuration Sections:**
- `global`: System-wide settings (logging, processing, metadata management)
- `sources`: Per-source configuration (URLs, formats, processing parameters)  
- `consolidation`: Metadata consolidation tool settings
- `docker`: Container resource limits and networking

### 3. Runtime Environment Variables - **System Overrides**

Direct environment variables allow runtime customization for specific hardware:

```bash
export OPENORGANELLE_CPU_LIMIT=4.0
export OPENORGANELLE_MEMORY_LIMIT=12g
export MAX_WORKERS=8      
```

### Configuration Priority Order:
1. **`.env` file** → Docker environment variables → Application
2. **`config/config.yaml`** as fallback when environment variables aren't set
3. **Runtime exports** override both `.env` and config.yaml

### OpenOrganelle Large Array Processing Configuration

The OpenOrganelle loader supports three processing modes for arrays >500MB, configured via `.env`:

```bash
# Process large arrays by streaming to Zarr format (recommended)
LARGE_ARRAY_MODE=stream
STREAMING_CHUNK_MB=2

# Process large arrays by downsampling to fit in memory  
LARGE_ARRAY_MODE=downsample
DOWNSAMPLE_FACTOR=4           # 4x reduction per pyramid level

# Skip large arrays entirely (emergency mode)
LARGE_ARRAY_MODE=skip
MAX_ARRAY_SIZE_MB=500         # Arrays above this size are skipped
```

**Resource Allocation Optimization:**
The system uses intelligent resource allocation based on workload characteristics:
- **OpenOrganelle**: 2.0 CPU cores, 6GB memory (optimized for 8GB Docker allocation)
- **EPFL**: 4.0 CPU cores, 10GB memory (high-parallelism download-intensive workload)
- **Small loaders** (IDR, FlyEM, EBI): 0.25-0.5 CPU cores, 1-1.5GB memory each

**OpenOrganelle Memory Configuration:**
For systems with 8GB+ Docker allocation, the OpenOrganelle loader can be optimized by setting environment variables:

```bash
# Optimize for 8GB Docker allocation
export OPENORGANELLE_MEMORY_LIMIT=6g
export MEMORY_LIMIT_GB=6
export MAX_ARRAY_SIZE_MB=1500
export ZARR_CHUNK_SIZE_MB=32
export MAX_WORKERS=2
```

Or create a `.env` file with:
```
OPENORGANELLE_MEMORY_LIMIT=6g
MEMORY_LIMIT_GB=6
MAX_ARRAY_SIZE_MB=1500
ZARR_CHUNK_SIZE_MB=32
MAX_WORKERS=2
```

This allows processing of much larger arrays (up to 1.5GB vs 500MB) with better performance while maintaining safety margins.

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

The system includes a comprehensive testing framework with parameterized loaders, unit tests, integration tests, and performance benchmarks that validate all aspects of the metadata v2.0 implementation.

**Test Architecture:**
- **Parameterized Loaders**: All hardcoded URLs and IDs extracted into configurable parameters
- **Unit Tests**: Fast tests with mocked external dependencies (✅ EBI, EPFL, FlyEM, IDR, OpenOrganelle)
- **Integration Tests**: End-to-end testing with real APIs (network required)
- **Performance Tests**: Benchmarking and load testing
- **Metadata Tests**: v2.0 schema validation and MetadataManager library testing
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

### Test Results & Reporting

The testing framework generates comprehensive reports in multiple formats:

**Test Report Outputs:**
- **HTML Reports**: `test_results/report.html` - Interactive test results with detailed failure information
- **JUnit XML**: `test_results/junit.xml` - Machine-readable test results for CI/CD integration
- **Coverage Reports**: `test_results/coverage/` - HTML coverage reports for lib/ and app/ modules
- **XML Coverage**: `coverage.xml` - Machine-readable coverage data for CI/CD integration
- **Performance Benchmarks**: Timing and throughput metrics for all loaders

**Current Test Status:**
- **Unit Tests**: All loaders tested with both mocked dependencies and real function testing
- **Real Function Coverage**: Significant improvement achieved across all loaders:
  - **IDR**: 35% coverage (up from 0%)
  - **EBI**: 46% coverage (up from 0%)
  - **EPFL**: 34% coverage (up from 0%)
  - **FlyEM**: 54% coverage (up from 0%)
  - **OpenOrganelle**: 9% coverage (up from 0%)
- **Integration Tests**: Network-dependent tests for API validation
- **Metadata Tests**: v2.0 schema compliance validated
- **Performance Tests**: Benchmarks for memory usage and processing speed

**Test Coverage Areas:**
- Metadata Manager library (v2.0 schema compliance)
- All 5 data loaders (EBI, EPFL, FlyEM, IDR, OpenOrganelle) with real implementation testing
- Consolidation tool (validation and reporting)
- Configuration management and error handling
- File format processing and data validation

### Real Function Testing Architecture

The testing framework employs a dual-testing approach that significantly improves code coverage by testing actual implementation code:

**Testing Approach:**
- **Mock-based Tests**: Traditional unit tests using mocked external dependencies for fast execution
- **Real Function Tests**: Import and test actual implementation functions from `app/*/main.py` modules
- **Dependency Mocking**: Strategic mocking of external libraries (requests, tifffile, zarr, etc.) while preserving real logic
- **Import Fallbacks**: Robust import strategies with fallback paths to handle missing dependencies

**Coverage Improvements:**
The real function testing approach achieved substantial coverage improvements across all loaders:
- **Systematic function imports**: Each test file imports core functions from the corresponding loader's main.py
- **TestReal*Functions classes**: Dedicated test classes for real implementation testing alongside traditional mock tests
- **Mocked external dependencies**: Strategic mocking of network calls, file I/O, and heavy dependencies while testing real business logic

**Test Structure:**
Each loader test file now contains:
- Traditional mock-based tests (e.g., `TestIDRLoader`) for configuration and integration scenarios
- Real function tests (e.g., `TestRealIDRFunctions`) that import and execute actual implementation code
- Systematic dependency mocking to allow imports without requiring actual external services

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
# Configuration files
.env.example                 # Environment configuration template
.env                         # Environment configuration (created from .env.example)
config/config.yaml           # YAML configuration (fallback values)
docker-compose.yml           # Docker service definitions

scripts/
├── run.sh                   # Main pipeline execution script
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

### Test Automation

The test framework supports automated execution with reporting:

```bash
# Generate JUnit XML for test reporting
python scripts/run_tests.py unit --junit-xml=test-results.xml

# Generate coverage reports
python scripts/run_tests.py unit --coverage

# Run fast tests only (for quick validation)
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