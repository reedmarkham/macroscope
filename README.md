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

## Metadata catalog

All the ingestion pipelines in this system produce a metadata JSON file describing each imaging dataset, written in two phases: first as a stub before saving the raw array, then enriched with computed statistics after the .npy volume has been saved. This design ensures recoverability and visibility of ingestion state. Each metadata file captures a consistent set of core fields across diverse sources (e.g., EMPIAR, IDR, EPFL, OpenOrganelle, FlyEM). 

These fields enable catalog-level search, filtering, and provenance tracking. Additional source-specific metadata is nested under the key `additional_metadata` to allow extensibility without breaking normalization. The `status` key can be used to group results in the catalog output, so we can see how the metadata looks between the in-process loads (i.e. expected and/or previously-developed schema) and completed loads (i.e. any developed but not cataloged keys).

For a concrete example of the distinct metadata keys across all data sources, as well as the presence of each key in each source's respective files, see the [aggregated metadata catalog example](./app/consolidate/metadata_catalog_20250603_045601.json).

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


To unify metadata across all ingested datasets, this repository includes a lightweight containerized tool that crawls the output directories of all ingestion pipelines (e.g., `app/ebi/empiar_volumes`, `app/idr/idr_volumes`, etc.), collects all `metadata*.json` files, and reports on the catalog using the keys of the JSON files. The consolidation process produces a timestamped file named `metadata_catalog_<TIMESTAMP>.json`, which aggregates all valid metadata records into a single searchable document for downstream indexing or visualization. It also enables analysis of data quality i.e. what % of files processed by source have each key of interest.

You can run this process manually:
```bash
cd/app/consolidate
docker build -t metadata-consolidator .
docker run --rm \
  -v "$PWD/../..:/repo" \
  -w /repo/app/consolidate \
  metadata-consolidator
```

## Monitoring:

When using `run.sh` (`docker compose`) logs are saved at the `logs/` subdirectory

## Development:

Building and running individual loaders locally (from the root of the repo) can be done like:
```
docker compose up --build openorganelle
```
Where the last string corresponds to the `docker-compose.yml`'s keys under `services:` (i.e. `ebi`, `epfl`, `flyem`, `idr`, `openorganelle`) for the service being developed.

## Future design considerations

* Increase robustness of metadata catalog tool (compare collected fields to expected, etc.)
* Adding CLI flags for passing content UUIDs, etc.
* Exposing pipelines as API endpoints
* Persist artifacts of data ingestion to manifest files to ease re-runs of pipeline
* Unit tests