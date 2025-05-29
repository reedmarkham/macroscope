# Proposal: ML & AI enablement using this repository

## Objective
Support ML/AI pipelines to access large volumetric image datasets and their associated metadata in a block-wise fashion (e.g., 128x128x128 voxels). 

---

## High-Level Architecture

### 1. **Storage Layer**
- **Backends Supported:**
  - Local filesystem
  - Cloud object stores (e.g., Amazon S3, GCS)
  - OME-Zarr or N5 formats for chunked storage
- **Preferred Format:** OME-Zarr, due to native support for chunked access and cloud compatibility

### 2. **Metadata Service**
- Centralized store for:
  - Dataset attributes (voxel size, dimensions, modality)
  - Acquisition metadata (source, date, microscope config)
  - Block index/cache
- **Implementation**: PostgreSQL

### 3. **Ingestion Engine**
- Downloads or reads raw datasets from source (e.g., EMPIAR, IDR, neuPrint)
- Converts to Zarr with 128x128x128 chunking
- Indexes metadata and stores it
- Supports parallelization for large datasets

### 4a. **Data Access API**
- RESTful or gRPC API to:
  - Request block: `/block?x=...&y=...&z=...&size=128`
  - Query metadata: `/metadata?dataset_id=...`
  - Enumerate datasets: `/datasets`
- Authentication support: JWT or API keys
- Rate limiting, logging, and usage tracking

### 4b. **Client SDK (Python)**
- Allows ML pipelines to:
  - Query available datasets
  - Load specific blocks as NumPy arrays
  - Stream data during training or preprocessing
- Abstracts away API and format details

---

## Workflow Overview

1. **Dataset Ingestion**
    - Triggered manually or via pipeline
    - Downloads source file (e.g. TIF, HDF5, N5, DM3)
    - Rechunks and stores as Zarr with standard block size (128x128x128)
    - Metadata and chunk map stored in Postgres

2. **ML Pipeline Consumption**
    - Requests dataset blocks using REST API or SDK
    - Loads data lazily as needed during training or inference
    - Can request full volume stats or bounding boxes
    - Also persist requested vectors to Postgres (`pgvector`) so that future requests check first

3. **Optional Enhancements**
    - LRU or disk-based block cache
    - Precomputed dataset pyramid for scale-aware access
    - Compression (e.g., Blosc or Zstd)
    - Monitoring and auto-scaling

---

## Deployment Considerations
- **Local (laptop, on-premise VM):**
  - Run via Docker Compose, mounting local data directory(s)

- **Cloud (AWS or GCP):**
  - Use ECS or GKE for service hosting
  - S3/GCS for data
  - RDS for metadata

---

## Tools & Technologies
| Component        | Tool/Stack               |
|------------------|--------------------------|
| Chunked Storage  | Zarr, N5                 |
| Metadata DB      | PostgreSQL, SQLAlchemy   |
| API              | FastAPI or gRPC          |
| Client SDK       | Python (NumPy)           |
| Deployment       | Docker, ECS (Fargate)    |

---

## Future Extensions
- Block-level access logs for caching optimization
- Annotation support overlay
- Integration with Dask or Ray for distributed training
- Federation across public neuroimaging datasets

---

## Summary
This design enables scalable, efficient access to high-resolution microscopy datasets in a format suitable for AI/ML pipelines. It abstracts storage and format complexities, supports local and cloud execution, and ensures modularity and extensibility for future use cases.
