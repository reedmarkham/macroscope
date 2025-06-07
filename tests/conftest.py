"""
Pytest configuration and shared fixtures for the electron microscopy ingestion tests.
"""

import pytest
import tempfile
import shutil
from pathlib import Path
from unittest.mock import Mock, patch
from typing import Dict, Any, Generator
import json
import os
import sys

# Add project root to path for imports
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lib.loader_config import (
    EBIConfig, EPFLConfig, FlyEMConfig, IDRConfig, OpenOrganelleConfig,
    load_test_configs, create_minimal_test_config
)
from lib.metadata_manager import MetadataManager
from lib.config_manager import ConfigManager


@pytest.fixture(scope="session")
def temp_test_dir() -> Generator[Path, None, None]:
    """Create a temporary directory for test outputs."""
    with tempfile.TemporaryDirectory(prefix="em_ingest_test_") as temp_dir:
        yield Path(temp_dir)


@pytest.fixture
def test_configs() -> Dict[str, Any]:
    """Load test configurations for all loaders."""
    return load_test_configs()


@pytest.fixture
def ebi_config(temp_test_dir: Path) -> EBIConfig:
    """EBI loader test configuration."""
    return create_minimal_test_config(
        'ebi',
        output_dir=str(temp_test_dir / "ebi"),
        timeout_seconds=60
    )


@pytest.fixture
def epfl_config(temp_test_dir: Path) -> EPFLConfig:
    """EPFL loader test configuration."""
    return create_minimal_test_config(
        'epfl',
        output_dir=str(temp_test_dir / "epfl"),
        timeout_seconds=60
    )


@pytest.fixture
def flyem_config(temp_test_dir: Path) -> FlyEMConfig:
    """FlyEM loader test configuration."""
    return create_minimal_test_config(
        'flyem',
        output_dir=str(temp_test_dir / "flyem"),
        crop_size=(50, 50, 50),  # Very small for fast testing
        timeout_seconds=60,
        random_seed=42
    )


@pytest.fixture
def idr_config(temp_test_dir: Path) -> IDRConfig:
    """IDR loader test configuration."""
    return create_minimal_test_config(
        'idr',
        output_dir=str(temp_test_dir / "idr"),
        timeout_seconds=60
    )


@pytest.fixture
def openorganelle_config(temp_test_dir: Path) -> OpenOrganelleConfig:
    """OpenOrganelle loader test configuration."""
    return create_minimal_test_config(
        'openorganelle',
        output_dir=str(temp_test_dir / "openorganelle"),
        timeout_seconds=60
    )


@pytest.fixture
def metadata_manager(temp_test_dir: Path) -> MetadataManager:
    """Create a metadata manager for testing."""
    # Use the main schema file
    schema_path = project_root / "schemas" / "metadata_schema.json"
    return MetadataManager(str(schema_path))


@pytest.fixture
def mock_requests():
    """Mock requests library for testing without network calls."""
    with patch('requests.get') as mock_get, \
         patch('requests.post') as mock_post:
        
        # Configure successful responses by default
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"status": "success"}
        mock_response.content = b"test content"
        mock_response.iter_content.return_value = [b"chunk1", b"chunk2"]
        
        mock_get.return_value = mock_response
        mock_post.return_value = mock_response
        
        yield {'get': mock_get, 'post': mock_post, 'response': mock_response}


@pytest.fixture
def mock_ftp():
    """Mock FTP operations for testing."""
    with patch('ftplib.FTP') as mock_ftp_class:
        mock_ftp_instance = Mock()
        mock_ftp_instance.nlst.return_value = ['test_file.dm3', 'another_file.mrc']
        mock_ftp_instance.size.return_value = 1024 * 1024  # 1MB
        
        def mock_retrbinary(cmd, callback):
            # Simulate file download
            test_data = b"test file content" * 1000
            callback(test_data)
        
        mock_ftp_instance.retrbinary = mock_retrbinary
        mock_ftp_class.return_value = mock_ftp_instance
        
        yield mock_ftp_instance


@pytest.fixture
def mock_s3fs():
    """Mock S3 filesystem for testing."""
    with patch('s3fs.S3FileSystem') as mock_s3_class:
        mock_s3_instance = Mock()
        mock_s3_instance.exists.return_value = True
        mock_s3_instance.ls.return_value = ['bucket/dataset.zarr/array1', 'bucket/dataset.zarr/array2']
        
        mock_s3_class.return_value = mock_s3_instance
        yield mock_s3_instance


@pytest.fixture
def mock_zarr():
    """Mock Zarr operations for testing."""
    import numpy as np
    
    with patch('zarr.open') as mock_zarr_open:
        # Create a mock zarr array
        mock_array = Mock()
        mock_array.shape = (100, 100, 100)
        mock_array.dtype = np.uint8
        mock_array.chunks = (32, 32, 32)
        mock_array.__getitem__ = lambda self, key: np.random.randint(0, 255, (50, 50, 50), dtype=np.uint8)
        
        mock_zarr_open.return_value = mock_array
        yield mock_array


@pytest.fixture
def mock_tiff_data():
    """Mock TIFF data for EPFL tests."""
    import numpy as np
    # Create realistic hippocampus volume dimensions
    return np.random.randint(0, 255, (100, 512, 512), dtype=np.uint8)


@pytest.fixture
def mock_ome_tiff_data():
    """Mock OME-TIFF data for IDR tests."""
    import numpy as np
    # Create realistic OME-TIFF volume with 16-bit data
    return np.random.randint(0, 65535, (50, 256, 256), dtype=np.uint16)


@pytest.fixture
def mock_idr_api_response():
    """Mock IDR API response data."""
    return {
        "data": {
            "@id": 9846137,
            "@type": "http://www.openmicroscopy.org/Schemas/OME/2016-06#Image",
            "Name": "test_image.tif",
            "SizeX": 256,
            "SizeY": 256,
            "SizeZ": 50,
            "PixelsPhysicalSizeX": {"@type": "TBD#LengthI", "Unit": "MICROMETER", "Value": 0.02},
            "PixelsPhysicalSizeY": {"@type": "TBD#LengthI", "Unit": "MICROMETER", "Value": 0.02},
            "PixelsPhysicalSizeZ": {"@type": "TBD#LengthI", "Unit": "MICROMETER", "Value": 0.02}
        },
        "pixels": {
            "pixelSizeX": 0.02,
            "pixelSizeY": 0.02,
            "pixelSizeZ": 0.02,
            "unit": "MICROMETER"
        }
    }


@pytest.fixture
def mock_zarr_data():
    """Mock Zarr data for OpenOrganelle tests."""
    import numpy as np
    # Create realistic volume data with larger dimensions
    return np.random.randint(0, 255, (100, 512, 512), dtype=np.uint8)


@pytest.fixture
def mock_zarr_operations(mock_zarr_data):
    """Mock Zarr operations for OpenOrganelle tests."""
    with patch('zarr.open') as mock_zarr_open:
        # Create mock zarr store that can handle string indexing
        mock_store = Mock()
        
        # Create mock array for specific paths
        mock_array = Mock()
        mock_array.shape = mock_zarr_data.shape
        mock_array.dtype = mock_zarr_data.dtype
        mock_array.chunks = (16, 32, 32)
        mock_array.__getitem__ = lambda self, key: mock_zarr_data[key] if hasattr(key, '__getitem__') else mock_zarr_data
        
        # Mock store should return the array for typical OpenOrganelle paths
        mock_store.__getitem__ = lambda self, key: mock_array if isinstance(key, str) else mock_zarr_data[key]
        
        mock_zarr_open.return_value = mock_store
        yield mock_store


@pytest.fixture
def sample_metadata() -> Dict[str, Any]:
    """Create sample metadata for testing."""
    return {
        "id": "test-uuid-12345",
        "source": "test",
        "source_id": "TEST-001",
        "status": "complete",
        "created_at": "2024-01-01T12:00:00Z",
        "updated_at": "2024-01-01T12:30:00Z",
        "metadata": {
            "core": {
                "description": "Test dataset for unit testing",
                "volume_shape": [100, 100, 100],
                "voxel_size_nm": [4.0, 4.0, 4.0],
                "data_type": "uint8"
            },
            "technical": {
                "file_size_bytes": 1000000,
                "sha256": "abcdef123456"
            }
        },
        "files": {
            "volume": "test_volume.npy",
            "metadata": "test_metadata.json"
        }
    }


@pytest.fixture
def create_test_file():
    """Factory fixture to create test files."""
    created_files = []
    
    def _create_file(path: Path, content: bytes = b"test content") -> Path:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'wb') as f:
            f.write(content)
        created_files.append(path)
        return path
    
    yield _create_file
    
    # Cleanup
    for file_path in created_files:
        if file_path.exists():
            file_path.unlink()


@pytest.fixture(autouse=True)
def setup_test_environment(temp_test_dir: Path):
    """Setup test environment variables and directories."""
    # Set test environment variables
    os.environ['EM_INGEST_TEST_MODE'] = 'true'
    os.environ['EM_INGEST_TEST_DIR'] = str(temp_test_dir)
    
    # Create test directory structure
    for subdir in ['ebi', 'epfl', 'flyem', 'idr', 'openorganelle']:
        (temp_test_dir / subdir).mkdir(parents=True, exist_ok=True)
    
    yield
    
    # Cleanup environment variables
    os.environ.pop('EM_INGEST_TEST_MODE', None)
    os.environ.pop('EM_INGEST_TEST_DIR', None)


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "integration: mark test as integration test (requires network)"
    )
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test (no external dependencies)"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers based on test names."""
    for item in items:
        # Add markers based on test file or function names
        if "integration" in item.nodeid:
            item.add_marker(pytest.mark.integration)
        elif "test_unit" in item.nodeid:
            item.add_marker(pytest.mark.unit)
        
        # Mark slow tests
        if any(keyword in item.nodeid for keyword in ["download", "large", "slow"]):
            item.add_marker(pytest.mark.slow)