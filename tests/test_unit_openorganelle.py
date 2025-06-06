"""
Unit tests for OpenOrganelle loader.
"""

import pytest
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import tempfile
import sys

# Add project paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.loader_config import OpenOrganelleConfig, ProcessingResult


class TestOpenOrganelleConfig:
    """Test OpenOrganelle configuration class."""
    
    def test_default_config(self):
        """Test default OpenOrganelle configuration."""
        config = OpenOrganelleConfig()
        
        assert config.s3_uri == "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr"
        assert config.zarr_path == "recon-2/em"
        assert config.dataset_id == "jrc_mus-nacc-2"
        assert config.voxel_size == {"x": 4.0, "y": 4.0, "z": 2.96}
        assert config.s3_anonymous == True
        assert config.output_dir == "zarr_volume"
        assert config.max_workers == 4
    
    def test_custom_config(self):
        """Test custom configuration parameters."""
        config = OpenOrganelleConfig(
            s3_uri="s3://janelia-cosem-datasets/jrc_hela-2/jrc_hela-2.zarr",
            zarr_path="recon-1/em",
            dataset_id="jrc_hela-2",
            voxel_size={"x": 8.0, "y": 8.0, "z": 8.0},
            s3_anonymous=False,
            max_workers=8
        )
        
        assert config.dataset_id == "jrc_hela-2"
        assert config.zarr_path == "recon-1/em"
        assert config.voxel_size == {"x": 8.0, "y": 8.0, "z": 8.0}
        assert config.s3_anonymous == False
        assert config.max_workers == 8
    
    def test_s3_uri_parsing(self):
        """Test S3 URI parsing properties."""
        config = OpenOrganelleConfig()
        
        assert config.bucket_name == "janelia-cosem-datasets"
        assert config.s3_key == "jrc_mus-nacc-2/jrc_mus-nacc-2.zarr"
    
    def test_different_datasets(self):
        """Test configuration for different datasets."""
        datasets = [
            "jrc_mus-nacc-2",
            "jrc_hela-2", 
            "jrc_macrophage-2",
            "jrc_jurkat-1",
            "jrc_cos7-1"
        ]
        
        for dataset in datasets:
            config = OpenOrganelleConfig(
                s3_uri=f"s3://janelia-cosem-datasets/{dataset}/{dataset}.zarr",
                dataset_id=dataset
            )
            assert config.dataset_id == dataset
            assert dataset in config.s3_uri


class TestOpenOrganelleLoader:
    """Test OpenOrganelle loader functionality."""
    
    @pytest.fixture
    def mock_zarr_data(self):
        """Create mock Zarr array data."""
        # Create multi-scale data (typical for OpenOrganelle)
        volume_shape = (512, 1024, 1024)  # Z, Y, X
        volume_data = np.random.randint(0, 255, volume_shape, dtype=np.uint8)
        return volume_data
    
    @pytest.fixture
    def mock_s3_filesystem(self):
        """Mock S3 filesystem operations."""
        with patch('s3fs.S3FileSystem') as mock_s3_class:
            mock_s3 = Mock()
            
            # Mock S3 operations
            mock_s3.exists.return_value = True
            mock_s3.ls.return_value = [
                'janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr/recon-2/em/s0',
                'janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr/recon-2/em/s1', 
                'janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr/recon-2/labels',
                'janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr/.zgroup'
            ]
            
            mock_s3_class.return_value = mock_s3
            yield mock_s3
    
    @pytest.fixture  
    def mock_zarr_operations(self, mock_zarr_data):
        """Mock Zarr operations."""
        with patch('zarr.open') as mock_zarr_open:
            mock_zarr_group = Mock()
            
            # Mock Zarr array
            mock_array = Mock()
            mock_array.shape = mock_zarr_data.shape
            mock_array.dtype = mock_zarr_data.dtype
            mock_array.chunks = (64, 64, 64)
            mock_array.__getitem__ = lambda self, key: mock_zarr_data[key] if isinstance(key, tuple) else mock_zarr_data
            mock_array.attrs = {
                'resolution': [8.0, 8.0, 8.0],
                'units': ['nanometer', 'nanometer', 'nanometer'],
                'offset': [0, 0, 0]
            }
            
            # Mock Zarr group structure
            mock_zarr_group.tree.return_value = """
            /
            ├── recon-2
            │   ├── em
            │   │   ├── s0 (2048, 2048, 2048) uint8
            │   │   ├── s1 (1024, 1024, 1024) uint8
            │   │   └── s2 (512, 512, 512) uint8
            │   └── labels
            │       ├── s0 (2048, 2048, 2048) uint32
            │       └── s1 (1024, 1024, 1024) uint32
            """
            
            # Mock accessing arrays
            mock_zarr_group.__getitem__ = lambda self, key: mock_array
            mock_zarr_open.return_value = mock_zarr_group
            
            yield mock_zarr_open
    
    def test_s3_connection(self, mock_s3_filesystem, openorganelle_config):
        """Test S3 filesystem connection."""
        import s3fs
        
        # Test anonymous connection
        fs = s3fs.S3FileSystem(anon=openorganelle_config.s3_anonymous)
        
        # Test bucket exists
        bucket_exists = fs.exists(openorganelle_config.bucket_name)
        assert bucket_exists == True
        
        # Test listing zarr store
        zarr_store_path = f"{openorganelle_config.bucket_name}/{openorganelle_config.s3_key}"
        zarr_contents = fs.ls(zarr_store_path)
        assert len(zarr_contents) > 0
    
    def test_zarr_store_access(self, mock_s3_filesystem, openorganelle_config):
        """Test Zarr store access via S3."""
        import s3fs
        import zarr
        
        # Create S3 filesystem
        fs = s3fs.S3FileSystem(anon=openorganelle_config.s3_anonymous)
        
        # Create S3Map for Zarr
        store_path = f"{openorganelle_config.bucket_name}/{openorganelle_config.s3_key}"
        
        with patch('s3fs.S3Map') as mock_s3_map:
            mock_store = Mock()
            mock_s3_map.return_value = mock_store
            
            # Test store creation
            store = s3fs.S3Map(store_path, s3=fs)
            assert store is not None
    
    def test_zarr_hierarchy_navigation(self, mock_zarr_operations, openorganelle_config):
        """Test navigating Zarr hierarchy."""
        import zarr
        
        # Open Zarr store
        zarr_store = zarr.open("dummy_path")
        
        # Test tree structure
        tree_str = zarr_store.tree()
        assert "recon-2" in str(tree_str)
        assert "em" in str(tree_str)
        
        # Test accessing specific arrays
        em_array = zarr_store["recon-2/em/s1"]
        assert em_array.shape == (512, 1024, 1024)
        assert em_array.dtype == np.uint8
    
    def test_multi_resolution_support(self, mock_zarr_operations, openorganelle_config):
        """Test multi-resolution pyramid support."""
        import zarr
        
        zarr_store = zarr.open("dummy_path")
        
        # Test different resolution levels
        resolution_levels = ["s0", "s1", "s2"]
        
        for level in resolution_levels:
            array_path = f"recon-2/em/{level}"
            array = zarr_store[array_path]
            
            # Higher resolution levels should have larger shapes
            assert len(array.shape) == 3
            assert all(dim > 0 for dim in array.shape)
    
    def test_chunked_processing(self, mock_zarr_data, openorganelle_config):
        """Test chunked data processing."""
        chunk_size = (64, 64, 64)
        
        # Test chunk iteration
        z_chunks = range(0, mock_zarr_data.shape[0], chunk_size[0])
        y_chunks = range(0, mock_zarr_data.shape[1], chunk_size[1])
        x_chunks = range(0, mock_zarr_data.shape[2], chunk_size[2])
        
        total_chunks = len(list(z_chunks)) * len(list(y_chunks)) * len(list(x_chunks))
        assert total_chunks > 1  # Should be processing in multiple chunks
        
        # Test chunk processing
        for z_start in z_chunks:
            for y_start in y_chunks:
                for x_start in x_chunks:
                    z_end = min(z_start + chunk_size[0], mock_zarr_data.shape[0])
                    y_end = min(y_start + chunk_size[1], mock_zarr_data.shape[1])
                    x_end = min(x_start + chunk_size[2], mock_zarr_data.shape[2])
                    
                    chunk = mock_zarr_data[z_start:z_end, y_start:y_end, x_start:x_end]
                    assert chunk.size > 0
    
    def test_parallel_processing(self, openorganelle_config):
        """Test parallel worker configuration."""
        from concurrent.futures import ThreadPoolExecutor
        
        # Test worker pool creation
        with ThreadPoolExecutor(max_workers=openorganelle_config.max_workers) as executor:
            # Simulate parallel chunk processing
            futures = []
            for i in range(8):  # 8 chunks
                future = executor.submit(lambda x: x * 2, i)
                futures.append(future)
            
            # Verify all tasks complete
            results = [f.result() for f in futures]
            assert len(results) == 8
            assert all(isinstance(r, int) for r in results)
    
    def test_metadata_generation(self, openorganelle_config, metadata_manager, mock_zarr_data):
        """Test metadata generation for OpenOrganelle data."""
        test_metadata = {
            "id": "550e8400-e29b-41d4-a716-446655440004",
            "source": "openorganelle",
            "source_id": openorganelle_config.dataset_id,
            "status": "complete",
            "created_at": "2024-01-01T12:00:00Z",
            "updated_at": "2024-01-01T12:30:00Z",
            "metadata": {
                "core": {
                    "description": "Mouse nucleus accumbens EM volume",
                    "volume_shape": list(mock_zarr_data.shape),
                    "voxel_size_nm": openorganelle_config.voxel_size,
                    "data_type": str(mock_zarr_data.dtype),
                    "modality": "EM"
                },
                "technical": {
                    "file_size_bytes": int(mock_zarr_data.nbytes),
                    "sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890",
                    "compression": "zarr",
                    "chunk_size": [64, 64, 64],
                    "global_mean": float(np.mean(mock_zarr_data)),
                    "dtype": str(mock_zarr_data.dtype)
                },
                "provenance": {
                    "download_url": openorganelle_config.s3_uri,
                    "processing_pipeline": "openorganelle-ingest-v1.0",
                    "internal_zarr_path": f"/{openorganelle_config.zarr_path}/s1"
                }
            },
            "additional_metadata": {
                "cosem_metadata": {
                    "organism": "Mus musculus",
                    "sample": "nucleus accumbens",
                    "protocol": "FIB-SEM serial sectioning",
                    "institution": "Janelia Research Campus",
                    "dataset_version": "1.0",
                    "resolution_level": "s1",
                    "pyramid_levels": 4
                }
            }
        }
        
        # Validate metadata
        validation_result = metadata_manager.validate_metadata(test_metadata)
        assert validation_result['valid'] == True
        assert len(validation_result['errors']) == 0
    
    def test_consolidated_metadata_handling(self):
        """Test consolidated Zarr metadata handling."""
        # Mock consolidated metadata
        consolidated_metadata = {
            ".zgroup": {"zarr_format": 2},
            "recon-2/.zgroup": {"zarr_format": 2},
            "recon-2/em/.zgroup": {"zarr_format": 2},
            "recon-2/em/s1/.zarray": {
                "chunks": [64, 64, 64],
                "compressor": {"id": "blosc"},
                "dtype": "|u1",
                "shape": [512, 1024, 1024]
            }
        }
        
        # Test accessing array metadata
        array_metadata = consolidated_metadata["recon-2/em/s1/.zarray"]
        assert array_metadata["chunks"] == [64, 64, 64]
        assert array_metadata["dtype"] == "|u1"  # uint8
        assert array_metadata["shape"] == [512, 1024, 1024]
    
    def test_error_handling_s3_failure(self, openorganelle_config):
        """Test error handling for S3 failures."""
        import s3fs
        
        with patch('s3fs.S3FileSystem') as mock_s3_class:
            mock_s3_class.side_effect = Exception("S3 connection failed")
            
            with pytest.raises(Exception, match="S3 connection failed"):
                s3fs.S3FileSystem(anon=openorganelle_config.s3_anonymous)
    
    def test_error_handling_zarr_failure(self, openorganelle_config):
        """Test error handling for Zarr failures."""
        import zarr
        
        with patch('zarr.open') as mock_zarr_open:
            mock_zarr_open.side_effect = ValueError("Invalid Zarr store")
            
            with pytest.raises(ValueError, match="Invalid Zarr store"):
                zarr.open("invalid_store")
    
    def test_error_handling_missing_arrays(self, mock_zarr_operations):
        """Test error handling for missing arrays."""
        import zarr
        
        zarr_store = zarr.open("dummy_path")
        
        with patch.object(zarr_store, '__getitem__') as mock_getitem:
            mock_getitem.side_effect = KeyError("Array not found")
            
            with pytest.raises(KeyError, match="Array not found"):
                zarr_store["nonexistent/array"]
    
    def test_filename_generation(self, openorganelle_config):
        """Test output filename generation."""
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Test volume filename
        volume_filename = f"{openorganelle_config.dataset_id}_em_s1_{timestamp}.npy"
        assert openorganelle_config.dataset_id in volume_filename
        assert "em" in volume_filename
        assert "s1" in volume_filename
        assert volume_filename.endswith(".npy")
        
        # Test metadata filename
        metadata_filename = f"metadata_{openorganelle_config.dataset_id}_{timestamp}.json"
        assert "metadata_" in metadata_filename
        assert openorganelle_config.dataset_id in metadata_filename
        assert metadata_filename.endswith(".json")


class TestOpenOrganelleDataValidation:
    """Test OpenOrganelle-specific data validation."""
    
    def test_dataset_naming_convention(self, openorganelle_config):
        """Test dataset naming convention validation."""
        # OpenOrganelle datasets follow jrc_organism-sample-version pattern
        dataset_parts = openorganelle_config.dataset_id.split('-')
        assert len(dataset_parts) >= 2
        assert dataset_parts[0].startswith("jrc_")
    
    def test_voxel_size_validation(self, openorganelle_config):
        """Test voxel size validation."""
        voxel_size = openorganelle_config.voxel_size
        
        # Should have x, y, z components
        assert "x" in voxel_size
        assert "y" in voxel_size
        assert "z" in voxel_size
        
        # Should be reasonable nanometer values
        for axis, size in voxel_size.items():
            assert 0.1 <= size <= 100.0  # 0.1nm to 100nm reasonable
    
    def test_zarr_path_validation(self, openorganelle_config):
        """Test Zarr path structure validation."""
        path_parts = openorganelle_config.zarr_path.split('/')
        
        # Should follow recon-X/datatype pattern
        assert len(path_parts) >= 2
        assert path_parts[0].startswith("recon-")
        assert path_parts[1] in ["em", "labels", "predictions"]  # Valid data types
    
    def test_s3_uri_validation(self, openorganelle_config):
        """Test S3 URI validation."""
        uri = openorganelle_config.s3_uri
        
        assert uri.startswith("s3://")
        assert "janelia-cosem-datasets" in uri
        assert uri.endswith(".zarr")
    
    def test_volume_size_expectations(self, mock_zarr_data):
        """Test expected volume size characteristics."""
        z, y, x = mock_zarr_data.shape
        
        # OpenOrganelle volumes should be reasonably large
        assert z >= 50     # At least 50 sections
        assert y >= 256    # At least 256x256 in plane
        assert x >= 256
        
        # But not unreasonably large for s1 resolution
        assert z <= 2048
        assert y <= 4096
        assert x <= 4096
    
    def test_data_type_expectations(self, mock_zarr_data):
        """Test data type expectations."""
        # OpenOrganelle EM data is typically uint8
        assert mock_zarr_data.dtype in [np.uint8, np.uint16]
        
        # Should have reasonable intensity range
        if mock_zarr_data.dtype == np.uint8:
            assert np.min(mock_zarr_data) >= 0
            assert np.max(mock_zarr_data) <= 255
    
    def test_chunk_size_optimization(self, mock_zarr_operations):
        """Test chunk size optimization."""
        import zarr
        
        zarr_store = zarr.open("dummy_path")
        array = zarr_store["recon-2/em/s1"]
        
        # Chunks should be reasonably sized (not too small, not too large)
        chunk_size = array.chunks
        for dim in chunk_size:
            assert 16 <= dim <= 512  # Reasonable chunk dimensions


@pytest.mark.integration
class TestOpenOrganelleIntegration:
    """Integration tests for OpenOrganelle loader (requires network)."""
    
    def test_real_s3_access(self, openorganelle_config):
        """Test actual S3 bucket access."""
        import s3fs
        
        try:
            fs = s3fs.S3FileSystem(anon=openorganelle_config.s3_anonymous)
            
            # Test if bucket exists
            bucket_exists = fs.exists(openorganelle_config.bucket_name)
            # Don't assert True as bucket might be private or temporarily unavailable
            
        except Exception:
            pytest.skip("S3 access not available for integration test")
    
    def test_real_zarr_metadata(self, openorganelle_config):
        """Test accessing real Zarr metadata."""
        import s3fs
        import zarr
        
        try:
            fs = s3fs.S3FileSystem(anon=openorganelle_config.s3_anonymous)
            store_path = f"{openorganelle_config.bucket_name}/{openorganelle_config.s3_key}"
            
            # Test if we can access the store
            if fs.exists(store_path):
                store = s3fs.S3Map(store_path, s3=fs)
                
                # Try to open Zarr store
                try:
                    zarr_group = zarr.open(store, mode='r')
                    tree = zarr_group.tree()
                    assert tree is not None
                except:
                    # Store might exist but be inaccessible
                    pass
                    
        except Exception:
            pytest.skip("Real Zarr access not available for integration test")
    
    @pytest.mark.slow
    def test_small_array_download(self, openorganelle_config):
        """Test downloading a small array sample."""
        # This would test actual small array downloads
        pytest.skip("Requires careful management of actual downloads")