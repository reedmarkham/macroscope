# electron-microscopy-ingest

A data pipeline for high-resolution electron microscopy images published by international institutions, generating high-dimensional vectors upstream of future ML & AI applications.

| Institution                             | Dataset / Description                                | URL                                                                                                                                                    | Format               | Access Method                                |
|-----------------------------------------|------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------|-----------------------------------------------|
| **Image Data Resource (OME)**           | IDR Dataset 9846137 (Hippocampus volume)             | https://idr.openmicroscopy.org/webclient/img_detail/9846137/?dataset=10740                                                                               | OME-TIFF             | OME REST API (HTTP⁺JSON)                     |
| **Electron Microscopy Public Image Archive (EMBL-EBI)** | EMPIAR-11759 (Mouse synapse volume; legacy .BM3)      | https://www.ebi.ac.uk/empiar/EMPIAR-11759/                                                                                                             | BM3 (legacy)         | FTP (ftp.ebi.ac.uk → /world_availability/)    |
| **CVLab (EPFL)**                        | CVLab EM dataset (Hippocampus TIFF stack)            | https://www.epfl.ch/labs/cvlab/data/data-em/                                                                                                            | TIFF stack           | Direct HTTP download                          |
| **Janelia Research Campus (HHMI)**      | OpenOrganelle: JRC_MUS-NACC-2 (Mouse Cortex Zarr)     | https://openorganelle.janelia.org/datasets/jrc_mus-nacc-2                                                                                                | Consolidated Zarr    | S3 (anonymous via s3fs)                       |
| **Janelia Research Campus (HHMI)**      | Hemibrain-NG random crop (Neuronal EM)               | https://tinyurl.com/hemibrain-ng                                                                                                                        | Zarr / Precomputed Blocks | HTTP (REST) or S3 (anonymous via s3fs)        |


In this pipeline, several different Python apps reflect the access methods above; they are containerized and locally run via a script in parallel using `docker compose`. We also leverage multithreading where possible to expedite the respective apps.

## Pre-requisites:

[Install Docker Compose](https://docs.docker.com/compose/install/)

## Execution:

```
chmod +x run.sh
sh run.sh
```

### Parallelization and multi-threading:
By default, when using `run.sh` (`docker compose`) these several containers will spin up and run in parallel on your local machine. Note the parallelization is constrained in the `docker-compose.yml` by hard-coding some CPU/memory usage guidelines in the context of an "average" laptop (I was using 2 GHz CPU / 16 GB memory), but further tuning can be done here.

Where possible, the loaders leverage multi-threading to speed up I/O and processing among multiple files from its source API or file/bucket. However the scripts are currently focused on loading isolated datasets for the proof-of-concept of this application.

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

### Common Metadata Fields

| Field              | Description                                                 | Sample Value                                                  |
|-------------------|-------------------------------------------------------------|---------------------------------------------------------------|
| `source`          | Data source identifier                                      | `"empiar"`                                                    |
| `source_id`       | Dataset identifier in source                                | `"EMPIAR-11759"`                                              |
| `description`     | Human-readable description                                  | `"High-resolution micrograph of a cell structure"`            |
| `volume_shape`    | Shape of the array (Z, Y, X)                                | `[512, 2048, 2048]`                                           |
| `voxel_size_nm`   | Physical resolution per axis (in nanometers)                | `[4.0, 4.0, 2.96]`                                            |
| `download_url`    | Original dataset location                                   | `"ftp://ftp.ebi.ac.uk/empiar/world_availability/11759/data/..."` |
| `local_paths`     | Paths to saved files (volume, raw, metadata)                | `{"volume": "vol_001.npy", "raw": "raw_001.tif", ...}`        |
| `status`          | Ingestion completion status                                 | `"complete"` (or `"saving-data"` / `"error: ..."` for stubs) |
| `timestamp`       | ISO UTC timestamp of creation                               | `"2024-05-30T20:53:12Z"`                                      |
| `sha256`          | Hash of the saved `.npy` file for integrity checking        | `"b15f7c9cb0d13d38..."`                                       |
| `file_size_bytes` | Size of saved `.npy` file in bytes                          | `2048123456`                                                  |
| `additional_metadata` | Source-specific structured metadata                     | `{ "title": "Fib-SEM image of mouse cortex", ... }`           |


### Enhanced Consolidation Tool

The metadata consolidation tool has been significantly upgraded to provide comprehensive metadata management:

**Enhanced Features:**
- **Schema Validation**: Validates all metadata against the formal JSON schema
- **Configuration-Driven**: Uses centralized YAML configuration for flexible operation
- **Quality Reporting**: Generates detailed validation reports and data quality metrics
- **Multiple Output Formats**: JSON catalogs, validation reports, and processing logs
- **Backward Compatibility**: Maintains legacy API while adding new capabilities

**Running the Consolidation Tool:**

**Enhanced Mode (Recommended):**
```bash
cd app/consolidate
python main.py --config ../../config/config.yaml
```

**Legacy Mode (Backward Compatible):**
```bash
cd app/consolidate
python main.py --legacy --root-dir ../..
```

**Docker Mode:**
```bash
cd app/consolidate
docker build -t metadata-consolidator .
docker run --rm \
  -v "$PWD/../..:/repo" \
  -w /repo/app/consolidate \
  metadata-consolidator
```

**Output Files:**
- `metadata_catalog_<TIMESTAMP>.json`: Enhanced catalog with validation statistics and quality metrics
- `validation_report_<TIMESTAMP>.json`: Detailed validation results for each metadata file
- `metadata_catalog_<TIMESTAMP>.log`: Processing log with detailed status information

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