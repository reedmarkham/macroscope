# electron-microscopy-ingest

A data pipeline for high-resolution electron microscopy images from online sources published by different institutions.

In this pipeline, several different Python apps are containerized and locally run via a script.

The sources from which we collect data include:
- [Janelia Research Campus, HHMI](https://www.janelia.org/)
- [Image Data Resource, OME](https://idr.openmicroscopy.org/)
- [Electron Microscopy Public Image Archive, EMBL-EBI](https://www.ebi.ac.uk/empiar/) - for now, older datasets using .BM3
- [CVLab, EPFL](https://www.epfl.ch/labs/cvlab/)

We also attempt to standardize metadata, such as:

TO-DO: document metadata catalog here

## Pre-requisites:

[Install Docker (compose)](https://docs.docker.com/compose/install/)

TO-DO: clarify below step

To use the neuprint Python library (for FlyEM data) you need to edit your local `.env` (it is ignored from the root of this repo):
```
NEUPRINT_TOKEN=...
```

## Execution:

```
chmod +x run.sh
sh run.sh
```

### Notes on parallelization and multi-threading:
By default, when using `run.sh` (`docker compose`) these several containers will spin up and run in parallel on your local machine. Note the parallelization is contrained in the `docker-compose.yml` by hard-coding some CPU/memory usage guidelines in the context of an "average" laptop, but further tuning can be done here.

Where possible, the loaders leverage multi-threading to speed up I/O and processing among multiple files from its source API or file/bucket. However the scripts are currently focused on loading isolated datasets for the proof-of-concept of this application.

## Monitoring:

When using `run.sh` (`docker compose`) logs are saved at the `logs/` subdirectory

## Development:

Building and running individual loaders can be done like:
```
docker compose up --build openorganelle
```
Where the last string corresponds to the `docker-compose.yml`'s keys under `services:` (i.e. `ebi`, `epfl`, `flyem`, `idr`, `openorganelle`) for the service being developed.

## Future design considerations

Persist artifacts of data ingestion to manifest files to ease re-runs of pipeline