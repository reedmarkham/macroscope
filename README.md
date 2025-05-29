# electron-microscopy-ingest

A data pipeline for high-resolution electron microscopy images from online sources published by different institutions.

In this pipeline, several different Python apps are containerized and locally run via a script.

The sources from which we collect data include:
- [Janelia Research Campus, HHMI](https://www.janelia.org/)
- [Image Data Resource, OME](https://idr.openmicroscopy.org/)
- [Electron Microscopy Public Image Archive, EMBL-EBI](https://www.ebi.ac.uk/empiar/)
- [CVLab, EPFL](https://www.epfl.ch/labs/cvlab/)

We also attempt to standardize metadata, such as:

## Pre-requisites:

[Install Docker (compose)](https://docs.docker.com/compose/install/)

To use the neuprint Python library (for FlyEM data) you need to first pass NEUPRINT_TOKEN to your environment variables:
```
export NEUPRINT_TOKEN=...
```

Optional: set additional environment variables (the `flyem` code will default to these values if not set here)
```
export NEUPRINT_SERVER=https://neuprint.janelia.org
export NEUPRINT_DATASET=hemibrain:v1.2.1
```

## Execution:

```
chmod +x run.sh
sh run.sh
```

### Notes on parallelization and multi-threading:
By default, using `docker-compose` these several containers will spin up and run in parallel.

The `ebi` (EMPIAR) loader leverages multi-threading to speed up I/O and processing among multiple files from its source API.

Multi-threading would not currently benefit the `flyem` (neuprint) and `epfl` loaders since they are working on 1 crop / file per run. 

There is opportunity to extend the `openorganelle` and `idr` loaders to support multi-threading if it is desired to batch load multiple crops, images - but currently they are also set up for singleton loads.

## Monitoring:

Logs are saved at the `logs/` subdirectory within each service's respective subdirectory i.e. `idr/logs/...`