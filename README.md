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

Each loader is containerized and runs in parallel using docker compose with multithreading for efficient I/O operations.

## Prerequisites

- [Docker Compose](https://docs.docker.com/compose/install/)
- Python 3.8+
- [Conda](https://docs.conda.io/en/latest/miniconda.html) (optional)

## Quick Start

Open Docker Desktop.

Execute orchestrated workflow of all 5 data loaders:
```bash
chmod +x scripts/run.sh
scripts/run.sh
```

## Execution Modes

The pipeline supports four execution modes via the `EM_EXECUTION_MODE` environment variable:

- **staged** (default): Light services parallel → Heavy services sequential
- **background**: EPFL in background + Sequential small loaders + OpenOrganelle overlap
- **parallel**: All services simultaneously
- **sequential**: One service at a time

```bash
# Default staged execution
scripts/run.sh

# Background orchestrated execution
EM_EXECUTION_MODE=background scripts/run.sh

# Parallel execution (high-memory systems)
EM_EXECUTION_MODE=parallel scripts/run.sh

# Sequential execution (memory conservative)
EM_EXECUTION_MODE=sequential scripts/run.sh
```

## Configuration

The system uses multi-layer configuration:

1. **Environment Configuration (.env file)**
```bash
cp .env.example .env
nano .env
```

2. **YAML Configuration (config/config.yaml)** - Fallback defaults

3. **Runtime Environment Variables** - System overrides

## Output Structure

```
data/
├── ebi/           # EMPIAR datasets
├── epfl/          # EPFL hippocampus data  
├── flyem/         # FlyEM hemibrain crops
├── idr/           # IDR OME-TIFF data
└── openorganelle/ # OpenOrganelle Zarr datasets

metadata/
├── metadata_catalog_*.json    # Metadata catalog
├── validation_report_*.json   # Validation results
└── metadata_catalog_*.log     # Processing logs

logs/
├── stage1_*.log     # Stage 1 execution logs
├── stage2_*.log     # Stage 2 execution logs
└── consolidate_*.log # Consolidation logs
```

## Metadata Management

The system features robust metadata management with:
- JSON Schema validation
- Standardized status tracking
- Centralized configuration
- Schema-compliant metadata generation

All metadata follows a common schema with core fields:
- **Core**: description, volume_shape, voxel_size_nm, data_type, modality
- **Technical**: file_size_bytes, sha256, compression, chunk_size
- **Provenance**: download_url, processing_pipeline, internal_zarr_path
- **Status**: pending, processing, saving-data, complete, failed, cancelled

## Development

Build and run individual loaders:
```bash
docker compose up --build openorganelle
```

Install Python dependencies:
```bash
pip install -r requirements.txt
```

## Project Structure

```
config/config.yaml           # Configuration
docker-compose.yml           # Docker services
scripts/run.sh              # Main pipeline script

lib/
├── loader_config.py        # Loader configurations
├── metadata_manager.py     # Metadata management
└── config_manager.py       # Configuration

app/
├── consolidate/            # Metadata consolidation
├── ebi/                   # EBI EMPIAR loader
├── epfl/                  # EPFL CVLab loader
├── flyem/                 # FlyEM DVID loader
├── idr/                   # IDR loader
└── openorganelle/         # OpenOrganelle loader
```