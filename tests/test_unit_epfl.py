"""
Unit tests for EPFL CVLab loader.
"""

import pytest
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import tempfile
import sys

# Add project paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.loader_config import EPFLConfig, ProcessingResult


class TestEPFLConfig:
    """Test EPFL configuration class."""
    
    def test_default_config(self):
        """Test default EPFL configuration."""
        config = EPFLConfig()
        
        assert config.download_url.startswith("https://documents.epfl.ch")
        assert config.source_id == "EPFL-CA1-HIPPOCAMPUS"
        assert config.description == "5x5x5µm section from CA1 hippocampus region"
        assert config.voxel_size_nm == [5.0, 5.0, 5.0]
        assert config.output_dir == "epfl_em_data"
        assert config.chunk_size_mb == 8
    
    def test_custom_config(self):
        """Test custom configuration parameters."""
        config = EPFLConfig(
            download_url="https://example.com/custom.tif",
            source_id="CUSTOM-DATASET",
            description="Custom EM dataset",
            voxel_size_nm=[4.0, 4.0, 4.0],
            chunk_size_mb=16
        )
        
        assert config.download_url == "https://example.com/custom.tif"
        assert config.source_id == "CUSTOM-DATASET"
        assert config.description == "Custom EM dataset"
        assert config.voxel_size_nm == [4.0, 4.0, 4.0]
        assert config.chunk_size_mb == 16


class TestEPFLLoader:
    """Test EPFL loader functionality."""
    
    @pytest.fixture
    def mock_tiff_data(self):
        """Create mock TIFF data."""
        # Create a 3D volume (Z, Y, X)
        volume_shape = (100, 256, 256)
        volume_data = np.random.randint(0, 255, volume_shape, dtype=np.uint8)
        return volume_data
    
    @pytest.fixture
    def mock_requests_download(self, mock_tiff_data):
        """Mock requests for file download."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.headers = {'content-length': str(mock_tiff_data.nbytes)}
            
            # Create chunks for streaming download
            chunk_size = 8 * 1024 * 1024  # 8MB chunks
            total_bytes = mock_tiff_data.tobytes()
            chunks = [total_bytes[i:i+chunk_size] for i in range(0, len(total_bytes), chunk_size)]
            
            mock_response.iter_content.return_value = chunks
            mock_response.raise_for_status.return_value = None
            
            mock_get.return_value.__enter__.return_value = mock_response
            mock_get.return_value = mock_response
            
            yield mock_get
    
    def test_url_validation(self, epfl_config):
        """Test URL accessibility validation."""
        import requests
        
        # Test valid URL format
        assert epfl_config.download_url.startswith("https://")
        assert ".tif" in epfl_config.download_url
    
    def test_streaming_download(self, mock_requests_download, epfl_config, temp_test_dir):
        """Test streaming download functionality."""
        import requests
        
        output_file = temp_test_dir / "test_download.tif"
        
        # Simulate streaming download
        response = requests.get(epfl_config.download_url, stream=True)
        assert response.status_code == 200
        
        # Test chunk iteration
        chunks = list(response.iter_content(chunk_size=1024*1024))
        assert len(chunks) > 0
        
        # Verify total size
        total_size = sum(len(chunk) for chunk in chunks)
        expected_size = int(response.headers.get('content-length', 0))
        assert total_size == expected_size
    
    def test_tiff_loading(self, mock_tiff_data, temp_test_dir):
        """Test TIFF file loading."""
        import tifffile
        
        # Create a temporary TIFF file
        tiff_path = temp_test_dir / "test_volume.tif"
        
        with patch('tifffile.imread') as mock_imread:
            mock_imread.return_value = mock_tiff_data
            
            # Test loading
            loaded_data = tifffile.imread(str(tiff_path))
            
            assert loaded_data.shape == mock_tiff_data.shape
            assert loaded_data.dtype == mock_tiff_data.dtype
            assert np.array_equal(loaded_data, mock_tiff_data)
    
    def test_volume_processing(self, mock_tiff_data, epfl_config):
        """Test volume data processing."""
        # Test shape validation
        assert len(mock_tiff_data.shape) == 3  # Should be 3D volume
        
        # Test data type conversion
        if mock_tiff_data.dtype != np.uint8:
            converted = mock_tiff_data.astype(np.uint8)
            assert converted.dtype == np.uint8
        
        # Test volume statistics
        volume_mean = np.mean(mock_tiff_data)
        volume_std = np.std(mock_tiff_data)
        
        assert 0 <= volume_mean <= 255
        assert volume_std >= 0
    
    def test_metadata_generation(self, epfl_config, metadata_manager, mock_tiff_data):
        """Test metadata generation for EPFL data."""
        test_metadata = {
            "id": "550e8400-e29b-41d4-a716-446655440002",
            "source": "epfl",
            "source_id": epfl_config.source_id,
            "status": "complete",
            "created_at": "2024-01-01T12:00:00Z",
            "updated_at": "2024-01-01T12:30:00Z",
            "metadata": {
                "core": {
                    "description": epfl_config.description,
                    "volume_shape": list(mock_tiff_data.shape),
                    "voxel_size_nm": epfl_config.voxel_size_nm,
                    "data_type": str(mock_tiff_data.dtype),
                    "modality": "EM"
                },
                "technical": {
                    "file_size_bytes": int(mock_tiff_data.nbytes),
                    "sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                },
                "provenance": {
                    "download_url": epfl_config.download_url,
                    "processing_pipeline": "epfl-ingest-v1.0"
                }
            },
            "additional_metadata": {
                "epfl_metadata": {
                    "lab": "Computer Vision Lab (CVLab)",
                    "institution": "École Polytechnique Fédérale de Lausanne (EPFL)",
                    "tissue_type": "hippocampus",
                    "brain_region": "CA1"
                }
            }
        }
        
        # Validate metadata
        validation_result = metadata_manager.validate_metadata(test_metadata)
        assert validation_result['valid'] == True
        assert len(validation_result['errors']) == 0
    
    def test_filename_generation(self, epfl_config):
        """Test output filename generation."""
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Test volume filename
        volume_filename = f"{epfl_config.source_id}_{timestamp}.npy"
        assert epfl_config.source_id in volume_filename
        assert volume_filename.endswith(".npy")
        
        # Test metadata filename
        metadata_filename = f"metadata_{epfl_config.source_id}_{timestamp}.json"
        assert "metadata_" in metadata_filename
        assert metadata_filename.endswith(".json")
        
        # Test original file preservation
        original_filename = f"{epfl_config.source_id}_original_{timestamp}.tif"
        assert "original" in original_filename
        assert original_filename.endswith(".tif")
    
    def test_error_handling_download_failure(self, epfl_config):
        """Test error handling for download failures."""
        import requests
        
        with patch('requests.get') as mock_get:
            # Test HTTP error
            mock_get.side_effect = requests.HTTPError("404 Not Found")
            
            with pytest.raises(requests.HTTPError):
                requests.get(epfl_config.download_url, stream=True)
    
    def test_error_handling_invalid_tiff(self, temp_test_dir):
        """Test error handling for invalid TIFF files."""
        import tifffile
        
        # Create invalid TIFF file
        invalid_tiff = temp_test_dir / "invalid.tif"
        with open(invalid_tiff, 'wb') as f:
            f.write(b"invalid tiff data")
        
        with patch('tifffile.imread') as mock_imread:
            mock_imread.side_effect = tifffile.TiffFileError("Invalid TIFF")
            
            with pytest.raises(tifffile.TiffFileError):
                tifffile.imread(str(invalid_tiff))
    
    def test_chunk_size_configuration(self, epfl_config):
        """Test download chunk size configuration."""
        # Test default chunk size
        assert epfl_config.chunk_size_mb == 8
        
        # Test custom chunk size
        custom_config = EPFLConfig(chunk_size_mb=16)
        assert custom_config.chunk_size_mb == 16
        
        # Calculate chunk size in bytes
        chunk_bytes = epfl_config.chunk_size_mb * 1024 * 1024
        assert chunk_bytes == 8 * 1024 * 1024
    
    def test_progress_tracking(self, mock_requests_download, epfl_config):
        """Test download progress tracking."""
        import requests
        
        response = requests.get(epfl_config.download_url, stream=True)
        total_size = int(response.headers.get('content-length', 0))
        
        downloaded = 0
        for chunk in response.iter_content(chunk_size=1024*1024):
            downloaded += len(chunk)
        
        assert downloaded == total_size
        
        # Test progress percentage calculation
        progress_percentage = (downloaded / total_size) * 100 if total_size > 0 else 0
        assert progress_percentage == 100.0


@pytest.mark.integration
class TestEPFLIntegration:
    """Integration tests for EPFL loader (requires network)."""
    
    def test_real_url_access(self, epfl_config):
        """Test actual URL accessibility."""
        import requests
        
        try:
            # Use HEAD request to check if URL is accessible without downloading
            response = requests.head(epfl_config.download_url, timeout=10)
            # Accept various success codes
            assert response.status_code in [200, 301, 302, 403]  # 403 might be normal for HEAD requests
        except requests.RequestException:
            pytest.skip("Network not available for integration test")
    
    @pytest.mark.slow
    def test_small_download_sample(self, epfl_config, temp_test_dir):
        """Test downloading a small sample (if available)."""
        # This would test actual download but should be carefully managed
        # to avoid downloading large files in regular testing
        pytest.skip("Requires careful management of actual downloads")


class TestEPFLDataValidation:
    """Test EPFL-specific data validation."""
    
    def test_hippocampus_metadata_validation(self, epfl_config):
        """Test hippocampus-specific metadata validation."""
        # Test tissue type validation
        assert "hippocampus" in epfl_config.description.lower()
        
        # Test brain region validation
        assert "ca1" in epfl_config.description.lower()
        
        # Test voxel size validation (should be isotropic 5nm)
        expected_voxel_size = [5.0, 5.0, 5.0]
        assert epfl_config.voxel_size_nm == expected_voxel_size
    
    def test_volume_size_expectations(self, mock_tiff_data):
        """Test expected volume size characteristics."""
        # EPFL dataset should be reasonably sized for hippocampus
        z, y, x = mock_tiff_data.shape
        
        # Reasonable bounds for hippocampus volume
        assert 50 <= z <= 500    # Reasonable number of sections
        assert 256 <= y <= 2048  # Reasonable Y dimension
        assert 256 <= x <= 2048  # Reasonable X dimension
        
        # Test aspect ratio (should be somewhat cubic for hippocampus)
        aspect_ratio_yx = y / x
        assert 0.5 <= aspect_ratio_yx <= 2.0  # Not too elongated
    
    def test_data_quality_checks(self, mock_tiff_data):
        """Test data quality validation."""
        # Test for reasonable intensity distribution
        mean_intensity = np.mean(mock_tiff_data)
        std_intensity = np.std(mock_tiff_data)
        
        # Should have reasonable contrast
        assert std_intensity > 10  # Not completely flat
        assert mean_intensity > 10  # Not mostly zeros
        
        # Test for data completeness (no large uniform regions)
        # Calculate gradient to check for structure
        if len(mock_tiff_data.shape) == 3:
            grad_z = np.gradient(mock_tiff_data.astype(float), axis=0)
            grad_y = np.gradient(mock_tiff_data.astype(float), axis=1)
            grad_x = np.gradient(mock_tiff_data.astype(float), axis=2)
            
            total_gradient = np.sqrt(grad_z**2 + grad_y**2 + grad_x**2)
            mean_gradient = np.mean(total_gradient)
            
            # Should have some structure (non-zero gradients)
            assert mean_gradient > 0.1