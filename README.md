# electron-microscopy-ingest

A data pipeline for high-resolution electron microscopy images from online sources published by different institutions.

In this pipeline, several different Python apps are containerized and locally run via a script.

The sources from which we collect data include:
- [Janelia Research Campus, HHMI](https://www.janelia.org/)
- [Image Data Resource, OME](https://idr.openmicroscopy.org/)
- [Electron Microscopy Public Image Archive, EMBL-EBI](https://www.ebi.ac.uk/empiar/) - for now, older datasets using .BM3
- [CVLab, EPFL](https://www.epfl.ch/labs/cvlab/)

Note: the `app/flyem/...` loader for Janelia's FlyEM dataset is still a work-in-progress.

## Pre-requisites:

[Install Docker Compose](https://docs.docker.com/compose/install/)

## Execution:

```
chmod +x run.sh
sh run.sh
```

### Notes on parallelization and multi-threading:
By default, when using `run.sh` (`docker compose`) these several containers will spin up and run in parallel on your local machine. Note the parallelization is contrained in the `docker-compose.yml` by hard-coding some CPU/memory usage guidelines in the context of an "average" laptop, but further tuning can be done here.

Where possible, the loaders leverage multi-threading to speed up I/O and processing among multiple files from its source API or file/bucket. However the scripts are currently focused on loading isolated datasets for the proof-of-concept of this application.

## Metadata catalog

All the ingestion pipelines in this system produce a metadata JSON file describing each imaging dataset, written in two phases: first as a stub before saving the raw array, then enriched with computed statistics after the .npy volume has been saved. This design ensures recoverability and visibility of ingestion state. Each metadata file captures a consistent set of core fields across diverse sources (e.g., EMPIAR, IDR, EPFL, OpenOrganelle, FlyEM). 

These fields enable catalog-level search, filtering, and provenance tracking. Additional source-specific metadata is nested under additional_metadata to allow extensibility without breaking normalization. The "status" field is used to group results in the catalog output, so we can see how the metadata looks between loading and completed load.

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


To unify metadata across all ingested datasets, this repository includes a lightweight containerized tool that crawls the output directories of all ingestion pipelines (e.g., `app/ebi/empiar_volumes`, `app/idr/idr_volumes`, etc.), collects all `metadata*.json` files, and validates them against the shared metadata schema.

Only metadata entries with `"status": "complete"` are included in the final catalog. Incomplete stubs (e.g., `"saving-data"` or `"error: ..."`) are logged and skipped. Each JSON file is checked for required fields such as `volume_shape`, `voxel_size_nm`, `download_url`, and `sha256`. Any missing or extra fields are reported during the crawl.

The consolidation process produces a timestamped file named `metadata_catalog_<TIMESTAMP>.json`, which aggregates all valid metadata records into a single searchable document for downstream indexing or visualization.

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

Building and running individual loaders can be done like:
```
docker compose up --build openorganelle
```
Where the last string corresponds to the `docker-compose.yml`'s keys under `services:` (i.e. `ebi`, `epfl`, `flyem`, `idr`, `openorganelle`) for the service being developed.

## Future design considerations

* Persist artifacts of data ingestion to manifest files to ease re-runs of pipeline
* Unit tests