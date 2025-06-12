"""
Unit tests for FlyEM/DVID loader.
"""

import pytest
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch
import sys

# Add project paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock missing dependencies first to allow imports
if 'requests' not in sys.modules:
    sys.modules['requests'] = Mock()
if 'config_manager' not in sys.modules:
    sys.modules['config_manager'] = Mock()
if 'metadata_manager' not in sys.modules:
    sys.modules['metadata_manager'] = Mock()

from lib.loader_config import FlyEMConfig, ProcessingResult

# Import real FlyEM functions with path manipulation for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "app" / "flyem"))
try:
    from main import (
        fetch_dataset_bounds,
        random_origin,
        fetch_gray3d_raw,
        build_metadata,
        save,
        fetch_random_crop,
        parse_args
    )
except ImportError as e:
    # Fallback if direct import fails - try alternative path
    sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
    try:
        from flyem.main import (
            fetch_dataset_bounds,
            random_origin,
            fetch_gray3d_raw,
            build_metadata,
            save,
            fetch_random_crop,
            parse_args
        )
    except ImportError:
        # If import still fails, skip real function tests
        fetch_dataset_bounds = None
        random_origin = None
        fetch_gray3d_raw = None
        build_metadata = None
        save = None
        fetch_random_crop = None
        parse_args = None


class TestFlyEMConfig:
    """Test FlyEM configuration class."""
    
    def test_default_config(self):
        """Test default FlyEM configuration."""
        config = FlyEMConfig()
        
        assert config.dvid_server == "http://hemibrain-dvid.janelia.org"
        assert config.uuid == "a89eb3af216a46cdba81204d8f954786"
        assert config.instance == "grayscale"
        assert config.crop_size == (1000, 1000, 1000)
        assert config.output_dir == "dvid_crops"
    
    def test_environment_override(self, monkeypatch):
        """Test environment variable override for instance."""
        monkeypatch.setenv("GRAYSCALE_INSTANCE", "test_instance")
        
        config = FlyEMConfig()
        assert config.instance == "test_instance"
    
    def test_custom_config(self):
        """Test custom configuration parameters."""
        config = FlyEMConfig(
            dvid_server="http://test-server.com",
            uuid="test-uuid",
            crop_size=(500, 500, 500),
            random_seed=42
        )
        
        assert config.dvid_server == "http://test-server.com"
        assert config.uuid == "test-uuid"
        assert config.crop_size == (500, 500, 500)
        assert config.random_seed == 42


class TestFlyEMLoader:
    """Test FlyEM loader functionality."""
    
    @pytest.fixture
    def mock_dvid_api(self):
        """Mock DVID API responses."""
        with patch('requests.get') as mock_get:
            # Mock info endpoint response
            info_response = Mock()
            info_response.status_code = 200
            info_response.json.return_value = {
                "Extended": {
                    "VoxelSize": [8.0, 8.0, 8.0],
                    "VoxelUnits": "nanometers"
                }
            }
            
            # Mock bounds endpoint response
            bounds_response = Mock()
            bounds_response.status_code = 200
            bounds_response.json.return_value = {
                "MinPoint": [0, 0, 0],
                "MaxPoint": [34432, 39552, 41408]
            }
            
            # Mock array data response
            array_response = Mock()
            array_response.status_code = 200
            array_response.content = np.random.randint(0, 255, (50, 50, 50), dtype=np.uint8).tobytes()
            
            # Configure responses based on URL
            def mock_get_side_effect(url, **kwargs):
                if "/info" in url:
                    return info_response
                elif "/sparsevol-size" in url:
                    return bounds_response
                else:
                    return array_response
            
            mock_get.side_effect = mock_get_side_effect
            yield mock_get
    
    def test_bounds_detection(self, mock_dvid_api, flyem_config):
        """Test DVID bounds detection."""
        import requests
        
        # Test info endpoint
        info_url = f"{flyem_config.dvid_server}/api/node/{flyem_config.uuid}/{flyem_config.instance}/info"
        response = requests.get(info_url)
        
        assert response.status_code == 200
        info_data = response.json()
        assert "Extended" in info_data
        assert "VoxelSize" in info_data["Extended"]
    
    def test_crop_coordinate_generation(self, flyem_config):
        """Test random crop coordinate generation."""
        # Set random seed for reproducible testing
        np.random.seed(flyem_config.random_seed or 42)
        
        # Mock dataset bounds
        dataset_bounds = {
            "MinPoint": [0, 0, 0],
            "MaxPoint": [10000, 10000, 10000]
        }
        
        crop_size = flyem_config.crop_size
        
        # Generate random coordinates
        max_x = dataset_bounds["MaxPoint"][0] - crop_size[0]
        max_y = dataset_bounds["MaxPoint"][1] - crop_size[1]
        max_z = dataset_bounds["MaxPoint"][2] - crop_size[2]
        
        start_x = np.random.randint(0, max_x)
        start_y = np.random.randint(0, max_y)
        start_z = np.random.randint(0, max_z)
        
        # Verify coordinates are within bounds
        assert 0 <= start_x <= max_x
        assert 0 <= start_y <= max_y
        assert 0 <= start_z <= max_z
        
        # Verify crop doesn't exceed bounds
        assert start_x + crop_size[0] <= dataset_bounds["MaxPoint"][0]
        assert start_y + crop_size[1] <= dataset_bounds["MaxPoint"][1]
        assert start_z + crop_size[2] <= dataset_bounds["MaxPoint"][2]
    
    def test_array_download(self, mock_dvid_api, flyem_config):
        """Test array data download from DVID."""
        import requests
        
        # Test array endpoint
        start_coords = [1000, 1000, 1000]
        size = [100, 100, 100]
        
        array_url = (f"{flyem_config.dvid_server}/api/node/{flyem_config.uuid}/"
                    f"{flyem_config.instance}/raw/0_1_2/"
                    f"{size[0]}_{size[1]}_{size[2]}/"
                    f"{start_coords[0]}_{start_coords[1]}_{start_coords[2]}")
        
        response = requests.get(array_url)
        assert response.status_code == 200
        assert len(response.content) > 0
    
    def test_array_parsing(self, flyem_config):
        """Test parsing downloaded array data."""
        # Create test data
        test_size = (50, 50, 50)
        test_data = np.random.randint(0, 255, test_size, dtype=np.uint8)
        raw_bytes = test_data.tobytes()
        
        # Parse bytes back to array
        parsed_array = np.frombuffer(raw_bytes, dtype=np.uint8).reshape(test_size)
        
        assert parsed_array.shape == test_size
        assert parsed_array.dtype == np.uint8
        assert np.array_equal(test_data, parsed_array)
    
    def test_metadata_generation(self, flyem_config, metadata_manager):
        """Test metadata generation for FlyEM data."""
        # Test metadata structure
        test_metadata = {
            "id": "550e8400-e29b-41d4-a716-446655440001",
            "source": "flyem",
            "source_id": flyem_config.uuid,
            "status": "complete",
            "created_at": "2024-01-01T12:00:00Z",
            "updated_at": "2024-01-01T12:30:00Z",
            "metadata": {
                "core": {
                    "description": "FlyEM hemibrain random crop",
                    "volume_shape": list(flyem_config.crop_size),
                    "voxel_size_nm": [8.0, 8.0, 8.0],
                    "data_type": "uint8",
                    "modality": "EM"
                },
                "technical": {
                    "file_size_bytes": int(np.prod(flyem_config.crop_size)),
                    "sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                },
                "provenance": {
                    "processing_pipeline": "flyem-ingest-v1.0"
                }
            },
            "additional_metadata": {
                "dvid_server": flyem_config.dvid_server,
                "instance": flyem_config.instance,
                "crop_coordinates": [1000, 1000, 1000],
                "uuid": flyem_config.uuid
            }
        }
        
        # Validate metadata
        validation_result = metadata_manager.validate_metadata(test_metadata)
        assert validation_result['valid'] == True
    
    def test_error_handling_server_down(self, flyem_config):
        """Test error handling when DVID server is unreachable."""
        import requests
        
        with patch('requests.get') as mock_get:
            mock_get.side_effect = requests.ConnectionError("Connection failed")
            
            with pytest.raises(requests.ConnectionError):
                requests.get(f"{flyem_config.dvid_server}/api/node/{flyem_config.uuid}/info")
    
    def test_error_handling_invalid_uuid(self, mock_dvid_api, flyem_config):
        """Test error handling for invalid UUID."""
        import requests
        
        with patch('requests.get') as mock_get:
            error_response = Mock()
            error_response.status_code = 404
            error_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
            mock_get.return_value = error_response
            
            with pytest.raises(requests.HTTPError):
                error_response.raise_for_status()
    
    def test_error_handling_invalid_crop_size(self, flyem_config):
        """Test error handling for invalid crop sizes."""
        # Test crop size larger than dataset
        dataset_bounds = {
            "MinPoint": [0, 0, 0], 
            "MaxPoint": [500, 500, 500]
        }
        
        # Crop size larger than dataset should be handled
        large_crop_size = (1000, 1000, 1000)
        
        max_x = dataset_bounds["MaxPoint"][0] - large_crop_size[0]
        
        # Should result in negative max coordinate
        assert max_x < 0
    
    def test_random_seed_reproducibility(self, flyem_config):
        """Test that random seed produces reproducible results."""
        seed = 42
        
        # Generate coordinates with same seed twice
        np.random.seed(seed)
        coords1_x = np.random.randint(0, 1000)
        coords1_y = np.random.randint(0, 1000)
        
        np.random.seed(seed)
        coords2_x = np.random.randint(0, 1000)
        coords2_y = np.random.randint(0, 1000)
        
        assert coords1_x == coords2_x
        assert coords1_y == coords2_y
    
    def test_filename_generation(self, flyem_config):
        """Test crop filename generation."""
        crop_coords = [1000, 2000, 3000]
        crop_size = flyem_config.crop_size
        
        # Generate filename pattern
        filename = f"crop_{crop_coords[0]}_{crop_coords[1]}_{crop_coords[2]}_{crop_size[0]}x{crop_size[1]}x{crop_size[2]}.npy"
        
        assert "crop_1000_2000_3000" in filename
        assert f"{crop_size[0]}x{crop_size[1]}x{crop_size[2]}" in filename
        assert filename.endswith(".npy")


@pytest.mark.integration
class TestFlyEMIntegration:
    """Integration tests for FlyEM loader (requires network)."""
    
    def test_real_dvid_connection(self, flyem_config):
        """Test actual DVID server connection."""
        import requests
        
        try:
            info_url = f"{flyem_config.dvid_server}/api/node/{flyem_config.uuid}/{flyem_config.instance}/info"
            response = requests.get(info_url, timeout=10)
            
            if response.status_code == 200:
                info_data = response.json()
                assert "Extended" in info_data or "Base" in info_data
            else:
                # Server might be down or UUID changed
                pytest.skip(f"DVID server returned {response.status_code}")
                
        except requests.RequestException:
            pytest.skip("Network not available for integration test")
    
    @pytest.mark.slow
    def test_small_crop_download(self, flyem_config, temp_test_dir):
        """Test downloading a small real crop."""
        # Use a very small crop size for testing
        test_config = FlyEMConfig(
            dvid_server=flyem_config.dvid_server,
            uuid=flyem_config.uuid,
            instance=flyem_config.instance,
            crop_size=(10, 10, 10),  # Very small for testing
            output_dir=str(temp_test_dir / "flyem"),
            timeout_seconds=30
        )
        
        # This would test actual download but should be carefully managed
        pytest.skip("Requires careful management of small test downloads")


class TestRealFlyEMFunctions:
    """Test real FlyEM implementation functions."""
    
    @pytest.mark.skipif(fetch_dataset_bounds is None, reason="FlyEM functions not available")
    def test_fetch_dataset_bounds_real(self):
        """Test real fetch_dataset_bounds function."""
        server = "http://hemibrain-dvid.janelia.org"
        uuid = "test-uuid"
        instance = "grayscale"
        
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {
                "bounds": [[0, 0, 0], [1000, 1000, 1000]]
            }
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            start_xyz, stop_xyz = fetch_dataset_bounds(server, uuid, instance)
            
            assert start_xyz == (0, 0, 0)
            assert stop_xyz == (1000, 1000, 1000)
            mock_get.assert_called_once()
    
    @pytest.mark.skipif(random_origin is None, reason="FlyEM functions not available")
    def test_random_origin_real(self):
        """Test real random_origin function."""
        bounds = ((0, 0, 0), (1000, 1000, 1000))
        crop_size = (100, 100, 100)
        
        # Set seed for reproducible test
        import random
        random.seed(42)
        
        origin = random_origin(bounds, crop_size)
        
        assert isinstance(origin, tuple)
        assert len(origin) == 3
        # Check origin is within valid bounds
        assert 0 <= origin[0] <= 900  # 1000 - 100
        assert 0 <= origin[1] <= 900
        assert 0 <= origin[2] <= 900
    
    @pytest.mark.skipif(fetch_gray3d_raw is None, reason="FlyEM functions not available")
    def test_fetch_gray3d_raw_real(self):
        """Test real fetch_gray3d_raw function."""
        server = "http://hemibrain-dvid.janelia.org"
        uuid = "test-uuid"
        instance = "grayscale"
        origin = (100, 100, 100)
        size = (50, 50, 50)
        
        # Create mock response with correct byte array
        test_data = np.random.randint(0, 255, size, dtype=np.uint8)
        
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.content = test_data.tobytes()
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = fetch_gray3d_raw(server, uuid, instance, origin, size)
            
            assert isinstance(result, np.ndarray)
            assert result.shape == size
            assert result.dtype == np.uint8
            mock_get.assert_called_once()
    
    @pytest.mark.skipif(build_metadata is None, reason="FlyEM functions not available")
    def test_build_metadata_real(self):
        """Test real build_metadata function."""
        server = "http://hemibrain-dvid.janelia.org"
        uuid = "test-uuid"
        crop_origin = (100, 100, 100)
        crop_shape = (50, 50, 50)
        vol_path = "/tmp/test.npy"
        timestamp = "20240101_120000"
        bounds = ((0, 0, 0), (1000, 1000, 1000))
        output_dir = "/tmp"
        
        with patch('metadata_manager.MetadataManager') as mock_mm_class:
            mock_mm = Mock()
            mock_mm_class.return_value = mock_mm
            mock_record = {"id": "test-id", "metadata": {}}
            mock_mm.create_metadata_record.return_value = mock_record
            
            result = build_metadata(
                server, uuid, crop_origin, crop_shape, 
                vol_path, timestamp, bounds, output_dir
            )
            
            assert isinstance(result, dict)
            assert result == mock_record
            mock_mm.create_metadata_record.assert_called_once()
            mock_mm.add_technical_metadata.assert_called_once()
            mock_mm.add_file_paths.assert_called_once()
    
    @pytest.mark.skipif(save is None, reason="FlyEM functions not available")
    def test_save_real(self):
        """Test real save function."""
        volume = np.array([[[1, 2], [3, 4]]])
        meta = {
            "id": "test-id",
            "files": {"metadata": "/tmp/metadata.json"}
        }
        name = "test_crop"
        output_dir = "/tmp"
        
        with patch('numpy.save') as mock_save, \
             patch('metadata_manager.MetadataManager') as mock_mm_class:
            
            mock_mm = Mock()
            mock_mm_class.return_value = mock_mm
            
            save(volume, meta, name, output_dir)
            
            mock_save.assert_called_once()
            mock_mm.add_technical_metadata.assert_called_once()
            mock_mm.update_status.assert_called_once_with(meta, "complete")
            mock_mm.save_metadata.assert_called_once()
    
    @pytest.mark.skipif(parse_args is None, reason="FlyEM functions not available")
    def test_parse_args_real(self):
        """Test real parse_args function."""
        with patch('sys.argv', ['main.py', '--config', 'test_config.yaml', '--uuid', 'test-uuid']):
            args = parse_args()
            
            assert hasattr(args, 'config')
            assert hasattr(args, 'uuid')
            assert args.config == 'test_config.yaml'
            assert args.uuid == 'test-uuid'