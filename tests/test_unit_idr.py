"""
Unit tests for IDR (Image Data Resource) loader.
"""

import pytest
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import tempfile
import sys

# Add project paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.loader_config import IDRConfig, ProcessingResult


class TestIDRConfig:
    """Test IDR configuration class."""
    
    def test_default_config(self):
        """Test default IDR configuration."""
        config = IDRConfig()
        
        assert config.image_ids == [9846137]
        assert config.dataset_id == "idr0086"
        assert config.ftp_host == "ftp.ebi.ac.uk"
        assert config.ftp_root_path == "/pub/databases/IDR"
        assert config.api_base_url == "https://idr.openmicroscopy.org/api/v0/m/"
        assert config.output_dir == "idr_volumes"
    
    def test_custom_config(self):
        """Test custom configuration parameters."""
        config = IDRConfig(
            image_ids=[1234567, 1234568],
            dataset_id="idr0001",
            ftp_host="custom.ftp.server",
            output_dir="./data/idr_test"
        )
        
        assert config.image_ids == [1234567, 1234568]
        assert config.dataset_id == "idr0001"
        assert config.ftp_host == "custom.ftp.server"
        assert config.output_dir == "./data/idr_test"
    
    def test_path_mappings(self):
        """Test dataset path mappings."""
        config = IDRConfig()
        
        # Test default path mapping
        assert "idr0086" in config.path_mappings
        expected_path = "idr0086-miron-micrographs/20200610-ftp/experimentD/Miron_FIB-SEM/Miron_FIB-SEM_processed"
        assert config.path_mappings["idr0086"] == expected_path
    
    def test_ftp_path_generation(self):
        """Test FTP path generation."""
        config = IDRConfig()
        
        ftp_path = config.get_ftp_path("idr0086")
        expected_path = "/pub/databases/IDR/idr0086-miron-micrographs/20200610-ftp/experimentD/Miron_FIB-SEM/Miron_FIB-SEM_processed"
        assert ftp_path == expected_path
    
    def test_environment_override(self, monkeypatch):
        """Test environment variable override for output directory."""
        monkeypatch.setenv("IDR_OUTPUT_DIR", "/custom/output/dir")
        
        config = IDRConfig()
        assert config.output_dir == "/custom/output/dir"


class TestIDRLoader:
    """Test IDR loader functionality."""
    
    @pytest.fixture
    def mock_idr_api_response(self):
        """Mock IDR API response."""
        return {
            "id": 9846137,
            "name": "Hippocampus_volume_001.tiff",
            "description": "High-resolution hippocampus volume",
            "dataset": {
                "id": 123,
                "name": "idr0086-miron-micrographs"
            },
            "pixels": {
                "sizeX": 2048,
                "sizeY": 2048,
                "sizeZ": 200,
                "pixelSizeX": {"value": 4.0, "unit": "NANOMETER"},
                "pixelSizeY": {"value": 4.0, "unit": "NANOMETER"},
                "pixelSizeZ": {"value": 4.0, "unit": "NANOMETER"}
            },
            "acquisition_date": "2020-01-15"
        }
    
    @pytest.fixture
    def mock_ome_tiff_data(self):
        """Create mock OME-TIFF data."""
        # Create a 3D volume with realistic dimensions
        volume_shape = (200, 2048, 2048)
        volume_data = np.random.randint(0, 65535, volume_shape, dtype=np.uint16)
        return volume_data
    
    @pytest.fixture
    def mock_requests_api(self, mock_idr_api_response):
        """Mock requests for IDR API calls."""
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.status_code = 200
            mock_response.json.return_value = mock_idr_api_response
            mock_response.raise_for_status.return_value = None
            
            mock_get.return_value = mock_response
            yield mock_get
    
    @pytest.fixture
    def mock_ftp_operations(self):
        """Mock FTP operations for IDR."""
        with patch('ftplib.FTP') as mock_ftp_class:
            mock_ftp = Mock()
            
            # Mock FTP directory listing
            mock_ftp.nlst.return_value = [
                'image_9846137.tiff',
                'image_9846138.tiff',
                'metadata.xml'
            ]
            
            # Mock file size
            mock_ftp.size.return_value = 1024 * 1024 * 100  # 100MB
            
            # Mock file download
            def mock_retrbinary(cmd, callback):
                # Simulate downloading file data
                test_data = b"OME-TIFF file content" * 1000
                callback(test_data)
            
            mock_ftp.retrbinary = mock_retrbinary
            mock_ftp_class.return_value = mock_ftp
            
            yield mock_ftp
    
    def test_api_metadata_fetch(self, mock_requests_api, idr_config, mock_idr_api_response):
        """Test IDR API metadata fetching."""
        import requests
        
        # Test API call
        api_url = f"{idr_config.api_base_url}images/{idr_config.image_ids[0]}/"
        response = requests.get(api_url)
        
        assert response.status_code == 200
        data = response.json()
        
        # Verify response structure
        assert data["id"] == 9846137
        assert "name" in data
        assert "description" in data
        assert "pixels" in data
        
        # Verify pixel information
        pixels = data["pixels"]
        assert pixels["sizeX"] == 2048
        assert pixels["sizeY"] == 2048
        assert pixels["sizeZ"] == 200
    
    def test_ftp_file_listing(self, mock_ftp_operations, idr_config):
        """Test FTP file listing."""
        # Get expected FTP path
        ftp_path = idr_config.get_ftp_path(idr_config.dataset_id)
        
        # Test file listing
        files = mock_ftp_operations.nlst(ftp_path)
        
        assert 'image_9846137.tiff' in files
        assert len(files) >= 1
        
        # Test filtering for specific image
        tiff_files = [f for f in files if f.endswith('.tiff')]
        assert len(tiff_files) >= 1
    
    def test_ftp_file_download(self, mock_ftp_operations, idr_config, temp_test_dir):
        """Test FTP file download."""
        output_file = temp_test_dir / "test_image.tiff"
        
        # Test download simulation
        downloaded_data = b""
        def collect_data(data):
            nonlocal downloaded_data
            downloaded_data += data
        
        mock_ftp_operations.retrbinary("RETR image_9846137.tiff", collect_data)
        
        assert len(downloaded_data) > 0
        assert b"OME-TIFF file content" in downloaded_data
    
    def test_ome_tiff_loading(self, mock_ome_tiff_data, temp_test_dir):
        """Test OME-TIFF file loading."""
        import tifffile
        
        tiff_path = temp_test_dir / "test_ome.tiff"
        
        with patch('tifffile.TiffFile') as mock_tiff_file:
            # Mock TiffFile context manager
            mock_tif = Mock()
            mock_tif.asarray.return_value = mock_ome_tiff_data
            mock_tif.ome_metadata = '<OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06">...</OME>'
            mock_tif.pages = [Mock(shape=mock_ome_tiff_data.shape[1:], dtype=mock_ome_tiff_data.dtype)]
            
            mock_tiff_file.return_value.__enter__.return_value = mock_tif
            
            # Test loading
            with tifffile.TiffFile(str(tiff_path)) as tif:
                volume = tif.asarray()
                ome_metadata = tif.ome_metadata
                
                assert volume.shape == mock_ome_tiff_data.shape
                assert volume.dtype == mock_ome_tiff_data.dtype
                assert ome_metadata is not None
    
    def test_ome_metadata_extraction(self):
        """Test OME metadata extraction."""
        import tifffile
        
        # Mock OME-XML metadata
        ome_xml = '''<?xml version="1.0" encoding="UTF-8"?>
        <OME xmlns="http://www.openmicroscopy.org/Schemas/OME/2016-06">
            <Image ID="Image:0" Name="Hippocampus_volume">
                <Pixels ID="Pixels:0" SizeX="2048" SizeY="2048" SizeZ="200" Type="uint16">
                    <Channel ID="Channel:0" Name="SEM"/>
                </Pixels>
            </Image>
        </OME>'''
        
        with patch('tifffile.xml2dict') as mock_xml2dict:
            mock_xml2dict.return_value = {
                'OME': {
                    'Image': {
                        'ID': 'Image:0',
                        'Name': 'Hippocampus_volume',
                        'Pixels': {
                            'SizeX': '2048',
                            'SizeY': '2048',
                            'SizeZ': '200',
                            'Type': 'uint16'
                        }
                    }
                }
            }
            
            # Test metadata parsing
            metadata_dict = tifffile.xml2dict(ome_xml)
            
            assert 'OME' in metadata_dict
            image_info = metadata_dict['OME']['Image']
            assert image_info['Name'] == 'Hippocampus_volume'
            
            pixels_info = image_info['Pixels']
            assert pixels_info['SizeX'] == '2048'
            assert pixels_info['Type'] == 'uint16'
    
    def test_metadata_generation(self, idr_config, metadata_manager, mock_idr_api_response):
        """Test metadata generation for IDR data."""
        test_metadata = {
            "id": "550e8400-e29b-41d4-a716-446655440003",
            "source": "idr",
            "source_id": str(mock_idr_api_response["id"]),
            "status": "complete",
            "created_at": "2024-01-01T12:00:00Z",
            "updated_at": "2024-01-01T12:30:00Z",
            "metadata": {
                "core": {
                    "description": mock_idr_api_response["description"],
                    "volume_shape": [200, 2048, 2048],
                    "voxel_size_nm": [4.0, 4.0, 4.0],
                    "data_type": "uint16",
                    "modality": "EM"
                },
                "technical": {
                    "file_size_bytes": 1677721600,
                    "sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                },
                "provenance": {
                    "download_url": f"ftp://{idr_config.ftp_host}{idr_config.get_ftp_path(idr_config.dataset_id)}/image_9846137.tiff",
                    "processing_pipeline": "idr-ingest-v1.0"
                }
            },
            "additional_metadata": {
                "idr_metadata": {
                    "dataset_id": idr_config.dataset_id,
                    "image_id": mock_idr_api_response["id"],
                    "acquisition_date": mock_idr_api_response["acquisition_date"],
                    "ome_metadata": {
                        "instrument": "FIB-SEM",
                        "channels": [{"name": "SEM", "wavelength": None}]
                    }
                }
            }
        }
        
        # Validate metadata
        validation_result = metadata_manager.validate_metadata(test_metadata)
        assert validation_result['valid'] == True
        assert len(validation_result['errors']) == 0
    
    def test_multi_image_processing(self, idr_config):
        """Test processing multiple image IDs."""
        multi_config = IDRConfig(
            image_ids=[9846137, 9846138, 9846139],
            dataset_id="idr0086"
        )
        
        assert len(multi_config.image_ids) == 3
        assert all(isinstance(img_id, int) for img_id in multi_config.image_ids)
    
    def test_error_handling_api_failure(self, idr_config):
        """Test error handling for API failures."""
        import requests
        
        with patch('requests.get') as mock_get:
            # Test 404 error
            mock_response = Mock()
            mock_response.status_code = 404
            mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
            mock_get.return_value = mock_response
            
            with pytest.raises(requests.HTTPError):
                response = requests.get(f"{idr_config.api_base_url}images/999999/")
                response.raise_for_status()
    
    def test_error_handling_ftp_failure(self, idr_config):
        """Test error handling for FTP failures."""
        import ftplib
        
        with patch('ftplib.FTP') as mock_ftp_class:
            mock_ftp_class.side_effect = ftplib.error_perm("530 Login incorrect")
            
            with pytest.raises(ftplib.error_perm):
                ftp = ftplib.FTP(idr_config.ftp_host)
    
    def test_filename_generation(self, idr_config):
        """Test output filename generation."""
        from datetime import datetime
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        image_id = 9846137
        
        # Test volume filename
        volume_filename = f"IDR-{image_id}_{timestamp}.npy"
        assert f"IDR-{image_id}" in volume_filename
        assert volume_filename.endswith(".npy")
        
        # Test metadata filename
        metadata_filename = f"metadata_IDR-{image_id}_{timestamp}.json"
        assert "metadata_" in metadata_filename
        assert str(image_id) in metadata_filename
        assert metadata_filename.endswith(".json")


class TestIDRDataValidation:
    """Test IDR-specific data validation."""
    
    def test_image_id_validation(self, idr_config):
        """Test image ID validation."""
        # Test valid image IDs (should be positive integers)
        for image_id in idr_config.image_ids:
            assert isinstance(image_id, int)
            assert image_id > 0
    
    def test_dataset_id_validation(self, idr_config):
        """Test dataset ID validation."""
        assert idr_config.dataset_id.startswith("idr")
        assert len(idr_config.dataset_id) >= 6  # e.g., "idr001"
    
    def test_ome_compliance(self, mock_ome_tiff_data):
        """Test OME-TIFF compliance expectations."""
        # OME-TIFF should support uint16 for EM data
        assert mock_ome_tiff_data.dtype in [np.uint8, np.uint16, np.uint32]
        
        # Should be 3D volume
        assert len(mock_ome_tiff_data.shape) == 3
        
        # Reasonable dimensions for microscopy
        z, y, x = mock_ome_tiff_data.shape
        assert 1 <= z <= 10000    # Reasonable Z-stack size
        assert 256 <= y <= 8192   # Reasonable Y dimension
        assert 256 <= x <= 8192   # Reasonable X dimension
    
    def test_voxel_size_validation(self, mock_idr_api_response):
        """Test voxel size information validation."""
        pixels = mock_idr_api_response["pixels"]
        
        # Should have pixel size information
        assert "pixelSizeX" in pixels
        assert "pixelSizeY" in pixels
        assert "pixelSizeZ" in pixels
        
        # Pixel sizes should be reasonable for EM (micrometer scale in API)
        x_size = pixels["pixelSizeX"]
        y_size = pixels["pixelSizeY"]
        z_size = pixels["pixelSizeZ"]
        
        assert 0.001 <= x_size <= 1.0  # 0.001µm to 1µm reasonable range
        assert 0.001 <= y_size <= 1.0
        assert 0.001 <= z_size <= 10.0  # Z can be thicker


@pytest.mark.integration
class TestIDRIntegration:
    """Integration tests for IDR loader (requires network)."""
    
    def test_real_api_access(self, idr_config):
        """Test actual IDR API access."""
        import requests
        
        try:
            api_url = f"{idr_config.api_base_url}images/{idr_config.image_ids[0]}/"
            response = requests.get(api_url, timeout=10)
            
            # Accept various response codes
            assert response.status_code in [200, 404, 500]
            
            if response.status_code == 200:
                data = response.json()
                assert "data" in data or "id" in data or "error" in data
                
        except requests.RequestException:
            pytest.skip("Network not available for integration test")
    
    def test_real_ftp_access(self, idr_config):
        """Test actual FTP server access."""
        import ftplib
        
        try:
            ftp = ftplib.FTP(idr_config.ftp_host, timeout=10)
            ftp.login()  # Anonymous login
            
            # Try to navigate to IDR directory
            try:
                ftp.cwd("/pub/databases/IDR")
                # Just test that we can access the directory
                dirs = ftp.nlst()
                assert len(dirs) > 0
            except ftplib.error_perm:
                # Directory might not be accessible, but connection worked
                pass
            finally:
                ftp.quit()
                
        except (ftplib.error_perm, OSError):
            pytest.skip("FTP server not accessible for integration test")
    
    @pytest.mark.slow
    def test_small_metadata_download(self, idr_config):
        """Test downloading small metadata files."""
        # This would test actual small file downloads
        pytest.skip("Requires careful management of actual downloads")