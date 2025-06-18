# Proposal: ML & AI Enablement

## Objective
Enable ML/AI pipelines to access large volumetric image datasets and their associated metadata in a block-wise fashion (e.g., 128x128x128 voxels). 

## Status

### Already Implemented
- Multi-source ingestion (5 sources: EBI, EPFL, FlyEM, IDR, OpenOrganelle)
- Metadata management with JSON schema validation (v2.0)
- Zarr format support (OpenOrganelle loader)
- Docker containerization with orchestrated execution
- Comprehensive configuration management
- Status tracking and processing states

### Partially Implemented
- Block-wise access (Zarr arrays available but no block API)
- Metadata database (JSON files, not PostgreSQL)
- Chunked storage (implemented for OpenOrganelle, needed for others)

### Missing Implementation
- RESTful/gRPC Data Access API
- Python Client SDK
- PostgreSQL metadata database
- 128³ standardized chunking across all sources
- Block-level caching with pgvector
- Authentication system

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

**AWS Implementation: On-Demand ECS Tasks**

**Institutional Loader Framework:**
- Base loader class with common functionality (Zarr conversion, metadata handling)
- Institution-specific implementations extending the base loader
- Plugin architecture for new data sources within institutions

**Example Structure:**
```
app/
├── base/
│   ├── loader.py          # BaseLoader class
│   ├── zarr_converter.py  # Common Zarr conversion
│   └── metadata_handler.py
├── institutions/
│   ├── ebi/
│   │   ├── empiar_loader.py    # EMPIAR datasets
│   │   ├── pride_loader.py     # PRIDE proteomics (future)
│   │   └── eva_loader.py       # EVA variants (future)
│   ├── janelia/
│   │   ├── flyem_loader.py     # FlyEM connectomics
│   │   ├── cosem_loader.py     # OpenOrganelle/COSEM
│   │   └── mouselight_loader.py # MouseLight (future)
│   ├── epfl/
│   │   ├── cvlab_loader.py     # Computer Vision Lab
│   │   └── bluebrain_loader.py # Blue Brain Project (future)
│   └── openmicroscopy/
│       ├── idr_loader.py       # Image Data Resource
│       └── omero_loader.py     # OMERO instances (future)
```

**On-Demand Execution Model:**
- API endpoint to trigger ingestion: `POST /ingest`
- ECS tasks launched dynamically based on request
- Request queuing with SQS for rate limiting
- Task status tracking via DynamoDB or RDS

**API-Driven Ingestion:**
```python
POST /api/v1/ingest
{
  "institution": "ebi",
  "loader": "empiar",
  "dataset_id": "11759",
  "priority": "normal",
  "output_location": "s3://em-data/ebi/"
}
```

**ECS Task Architecture:**
```
API Gateway → Lambda → SQS → ECS Task (institution/loader) → S3 + RDS
```

### 4a. **Data Access API**
- RESTful API for block-wise data access
- Authentication support: JWT or API keys  
- Rate limiting, logging, and usage tracking

**AWS Implementation: FastAPI on ECS**

**Service Architecture:**
- FastAPI application containerized and deployed on ECS Fargate
- Application Load Balancer (ALB) for high availability and SSL termination
- Auto Scaling based on CPU/memory utilization and request count
- CloudWatch for monitoring and alerting

**API Endpoints:**
```python
GET /api/v1/block?dataset_id=...&x=...&y=...&z=...&size=128
GET /api/v1/metadata?dataset_id=...
GET /api/v1/datasets?institution=...&modality=...
POST /api/v1/ingest  # Trigger new dataset ingestion
GET /api/v1/status?job_id=...  # Check ingestion status
```

**Deployment Configuration:**
```yaml
# ECS Service Configuration
Service: em-data-api
  Platform: Fargate
  CPU: 1 vCPU (scalable to 4 vCPU)
  Memory: 2 GB (scalable to 8 GB)
  Auto Scaling:
    Min: 2 tasks
    Max: 10 tasks
    Target CPU: 70%
    Target Memory: 80%
  Health Check: /health
  Load Balancer: ALB with SSL certificate
```

**Performance Optimizations:**
- ElastiCache (Redis) for block-level caching
- S3 Transfer Acceleration for faster data access
- Connection pooling to RDS PostgreSQL
- Async/await patterns for I/O operations

### 4b. **Client SDK (Python)**
- Allows ML pipelines to:
  - Query available datasets
  - Load specific blocks as NumPy arrays
  - Stream data during training or preprocessing
- Abstracts away API and format details

---

## Workflow Overview

1. **Dataset Ingestion**
    - Triggered via API: `POST /api/v1/ingest`
    - Request specifies institution, loader, and dataset_id
    - Lambda function validates request and queues ECS task via SQS
    - ECS task downloads source file (e.g. TIF, HDF5, N5, DM3)
    - Converts and rechunks to OME-Zarr with 128³ blocks
    - Stores data in S3 and metadata in RDS PostgreSQL

2. **ML Pipeline Consumption**
    - Uses Python SDK to request blocks: `client.get_block(dataset_id, x, y, z)`
    - SDK queries FastAPI service: `GET /api/v1/block`
    - ElastiCache Redis checked first for cached blocks
    - S3 data accessed via Zarr if not cached
    - Block access patterns stored in pgvector for optimization

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
| Component        | Tool/Stack                           |
|------------------|--------------------------------------|
| Chunked Storage  | OME-Zarr, N5                        |
| Metadata DB      | RDS PostgreSQL, pgvector, SQLAlchemy|
| API              | FastAPI on ECS Fargate              |
| Client SDK       | Python (NumPy, requests)            |
| Deployment       | Docker, ECS Fargate, ALB            |
| Orchestration    | API Gateway, Lambda, SQS, EventBridge|
| Caching          | ElastiCache Redis                    |
| Storage          | S3, S3 Transfer Acceleration        |
| Monitoring       | CloudWatch, CloudWatch Logs         |

---

## Implementation Roadmap

### Phase 1: Data Standardization (2-3 weeks)
1. **Implement Zarr conversion for all loaders**: Currently only OpenOrganelle outputs Zarr. Convert EBI/EPFL/FlyEM/IDR outputs to OME-Zarr with 128³ chunks.
   - Location: `app/*/main.py` - add Zarr export functions
   - Files to modify: `app/ebi/main.py:712`, `app/epfl/main.py:445`, etc.

2. **Standardize chunk sizes**: Update `config/config.yaml` processing.chunk_size_mb across all sources to target 128³ voxel chunks.

### Phase 2: Database Migration (1-2 weeks)
1. **PostgreSQL integration**: Replace JSON metadata files with PostgreSQL database
   - Add `docker-compose.yml` PostgreSQL service
   - Create migration scripts in `scripts/migrate_to_postgres.py`
   - Update `lib/metadata_manager.py` with SQLAlchemy ORM

2. **pgvector integration**: Add vector similarity search for requested blocks
   - Schema: `CREATE EXTENSION vector; ALTER TABLE blocks ADD COLUMN embedding vector(512);`

### Phase 3: API Development (3-4 weeks)
1. **FastAPI service**: Create `app/api/` directory with RESTful endpoints
   ```python
   # app/api/main.py
   @app.get("/block")
   async def get_block(x: int, y: int, z: int, size: int = 128, dataset_id: str):
       # Implementation here
   ```

2. **Block indexing**: Create `lib/block_indexer.py` to pre-compute available blocks per dataset

### Phase 4: Client SDK (1-2 weeks)
1. **Python SDK**: Create `sdk/python/em_client/` package
   ```python
   # Usage example
   from em_client import EMClient
   client = EMClient("http://localhost:8000")
   block = client.get_block(dataset_id="ebi_11759", x=0, y=0, z=0)
   ```

## System Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ML/AI Data Access System                           │
└─────────────────────────────────────────────────────────────────────────────────┘

┌──────────────────┐    ┌──────────────────┐    ┌──────────────────┐
│   Data Sources   │    │  Ingestion Layer │    │  Storage Layer   │
│                  │    │  (ECS Tasks)     │    │    (AWS S3)      │
│ ┌──────────────┐ │    │ ┌──────────────┐ │    │ ┌──────────────┐ │
│ │ EBI EMPIAR   │ │────│ │ EBI Institute│ │────│ │  OME-Zarr    │ │
│ │ EBI PRIDE    │ │    │ │ - empiar     │ │    │ │ 128³ chunks  │ │
│ │ EBI EVA      │ │    │ │ - pride      │ │    │ │ S3 buckets   │ │
│ └──────────────┘ │    │ └──────────────┘ │    │ └──────────────┘ │
│                  │    │                  │    │                  │
│ ┌──────────────┐ │    │ ┌──────────────┐ │    │ ┌──────────────┐ │
│ │ Janelia      │ │────│ │ Janelia Inst │ │────│ │  OME-Zarr    │ │
│ │ - FlyEM      │ │    │ │ - flyem      │ │    │ │ 128³ chunks  │ │
│ │ - COSEM      │ │    │ │ - cosem      │ │    │ │ S3 buckets   │ │
│ └──────────────┘ │    │ └──────────────┘ │    │ └──────────────┘ │
│                  │    │                  │    │                  │
│ ┌──────────────┐ │    │ ┌──────────────┐ │    │ ┌──────────────┐ │
│ │ EPFL CVLab   │ │────│ │ EPFL Inst    │ │────│ │  OME-Zarr    │ │
│ │ Blue Brain   │ │    │ │ - cvlab      │ │    │ │ 128³ chunks  │ │
│ │ Project      │ │    │ │ - bluebrain  │ │    │ │ S3 buckets   │ │
│ └──────────────┘ │    │ └──────────────┘ │    │ └──────────────┘ │
│                  │    │                  │    │                  │
│ ┌──────────────┐ │    │ ┌──────────────┐ │    │ ┌──────────────┐ │
│ │ OME IDR      │ │────│ │ OME Inst     │ │────│ │  OME-Zarr    │ │
│ │ OMERO Hubs   │ │    │ │ - idr        │ │    │ │ 128³ chunks  │ │
│ │              │ │    │ │ - omero      │ │    │ │ S3 buckets   │ │
│ └──────────────┘ │    │ └──────────────┘ │    │ └──────────────┘ │
└──────────────────┘    └──────────────────┘    └──────────────────┘
         │                        │                        │
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                         AWS Orchestration Layer                                 │
│                                                                                 │
│ ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────┐ │
│ │  API Gateway    │  │     Lambda      │  │      SQS        │  │ EventBridge │ │
│ │ - /ingest       │  │ - Task Launcher │  │ - Job Queue     │  │ - Triggers  │ │
│ │ - Rate Limiting │  │ - Validation    │  │ - Dead Letter   │  │ - Schedules │ │
│ └─────────────────┘  └─────────────────┘  └─────────────────┘  └─────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
         │                        │                        │
         │                        │                        │
         ▼                        ▼                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                            Metadata & Access Layer                              │
│                                                                                 │
│ ┌─────────────────────────┐  ┌─────────────────────────┐  ┌──────────────────┐ │
│ │   RDS PostgreSQL DB     │  │    ElastiCache Redis    │  │  FastAPI on ECS  │ │
│ │                         │  │                         │  │    (Fargate)     │ │
│ │ ┌─────────────────────┐ │  │ ┌─────────────────────┐ │  │ ┌──────────────┐ │ │
│ │ │  Datasets Table     │ │  │ │   Block Cache       │ │  │ │GET /api/v1/  │ │ │
│ │ │ - id, institution   │ │  │ │ - Redis KV Store    │ │  │ │  /block      │ │ │
│ │ │ - loader, shape     │ │  │ │ - LRU Eviction      │ │  │ │  /metadata   │ │ │
│ │ │ - voxel_size_nm     │ │  │ │                     │ │  │ │  /datasets   │ │ │
│ │ └─────────────────────┘ │  │ └─────────────────────┘ │  │ └──────────────┘ │ │
│ │                         │  │                         │  │                  │ │
│ │ ┌─────────────────────┐ │  │ ┌─────────────────────┐ │  │ ┌──────────────┐ │ │
│ │ │   Blocks Table      │ │  │ │   pgvector Store    │ │  │ │POST /api/v1/ │ │ │
│ │ │ - x, y, z coords    │ │  │ │ - block embeddings  │ │  │ │  /ingest     │ │ │
│ │ │ - dataset_id        │ │  │ │ - similarity search │ │  │ │GET /api/v1/  │ │ │
│ │ │ - zarr_s3_path      │ │  │ │                     │ │  │ │  /status     │ │ │
│ │ └─────────────────────┘ │  │ └─────────────────────┘ │  │ └──────────────┘ │ │
│ └─────────────────────────┘  └─────────────────────────┘  └──────────────────┘ │
│                                                                                 │
│ ┌─────────────────────────────────────────────────────────────────────────────┐ │
│ │                            Application Load Balancer (ALB)                  │ │
│ │ - SSL Termination  - Health Checks  - Auto Scaling Triggers  - Routing     │ │
│ └─────────────────────────────────────────────────────────────────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Client SDK Layer                                   │
│                                                                                 │
│ ┌─────────────────────────┐  ┌─────────────────────────┐  ┌──────────────────┐ │
│ │    Python SDK           │  │    ML Pipeline Client   │  │  Jupyter Client  │ │
│ │                         │  │                         │  │                  │ │
│ │ ```python               │  │ ```python               │  │ ```python        │ │
│ │ from em_client import   │  │ def training_loop():    │  │ client = EMClient│ │
│ │   EMClient              │  │   client = EMClient()   │  │ datasets = client│ │
│ │                         │  │   for batch in client   │  │   .list_datasets()│ │
│ │ client = EMClient(      │  │     .stream_blocks(     │  │ vol = client.get │ │
│ │   "http://api:8000")    │  │       dataset="ebi_123",│  │   _volume(...)   │ │
│ │                         │  │       batch_size=32):   │  │ plt.imshow(vol[0])│ │
│ │ block = client.get_block│  │     train_step(batch)   │  │ ```              │ │
│ │   (x=0, y=0, z=0)       │  │ ```                     │  │                  │ │
│ │ ```                     │  │                         │  │                  │ │
│ └─────────────────────────┘  └─────────────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────────────────────────────────────────┘
```

## Key Implementation Files

### Database Schema (`migrations/001_create_tables.sql`)
```sql
CREATE TABLE datasets (
    id UUID PRIMARY KEY,
    source VARCHAR(50) NOT NULL,
    source_id VARCHAR(255) NOT NULL,
    volume_shape INTEGER[],
    voxel_size_nm DECIMAL[],
    zarr_path TEXT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE blocks (
    id UUID PRIMARY KEY,
    dataset_id UUID REFERENCES datasets(id),
    x INTEGER, y INTEGER, z INTEGER,
    size_x INTEGER DEFAULT 128,
    size_y INTEGER DEFAULT 128, 
    size_z INTEGER DEFAULT 128,
    zarr_chunk_path TEXT,
    embedding vector(512),
    access_count INTEGER DEFAULT 0,
    last_accessed TIMESTAMP DEFAULT NOW()
);
```

### API Service (`app/api/main.py`)
```python
from fastapi import FastAPI, HTTPException
import zarr
import numpy as np

app = FastAPI(title="EM Data Access API")

@app.get("/api/v1/block")
async def get_block(dataset_id: str, x: int, y: int, z: int, size: int = 128):
    # Get zarr path from database
    zarr_path = get_zarr_path(dataset_id)
    store = zarr.open(zarr_path, mode='r')
    
    # Extract block
    block_data = store[z:z+size, y:y+size, x:x+size]
    
    # Log access for caching optimization  
    log_block_access(dataset_id, x, y, z)
    
    return {
        "data": block_data.tolist(),
        "shape": block_data.shape,
        "dtype": str(block_data.dtype)
    }
```

### Client SDK (`sdk/python/em_client/client.py`)
```python
import requests
import numpy as np

class EMClient:
    def __init__(self, base_url: str, api_key: str = None):
        self.base_url = base_url
        self.headers = {"Authorization": f"Bearer {api_key}"} if api_key else {}
    
    def get_block(self, dataset_id: str, x: int, y: int, z: int, size: int = 128) -> np.ndarray:
        response = requests.get(
            f"{self.base_url}/api/v1/block",
            params={"dataset_id": dataset_id, "x": x, "y": y, "z": z, "size": size},
            headers=self.headers
        )
        data = response.json()
        return np.array(data["data"])
    
    def stream_blocks(self, dataset_id: str, batch_size: int = 32):
        # Implementation for streaming data during ML training
        pass
```

## Future Extensions
- Block-level access logs for caching optimization
- Annotation support overlay
- Integration with Dask or Ray for distributed training
- Federation across public neuroimaging datasets

---

## Summary
This design enables scalable, efficient access to high-resolution microscopy datasets in a format suitable for AI/ML pipelines. It abstracts storage and format complexities, supports local and cloud execution, and ensures modularity and extensibility for future use cases. The implementation leverages the existing robust v2.0 metadata infrastructure and Docker architecture while adding the ML/AI capabilities outlined in this proposal.
