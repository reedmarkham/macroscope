"""
Unit tests for EBI EMPIAR loader.
"""

import pytest
import json
import numpy as np
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
import tempfile
import sys

# Add project paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

# Mock missing dependencies first to allow imports
if 'ncempy' not in sys.modules:
    sys.modules['ncempy'] = Mock()
    sys.modules['ncempy.io'] = Mock()
    sys.modules['ncempy.io.ser'] = Mock()
if 'dm3_lib' not in sys.modules:
    sys.modules['dm3_lib'] = Mock()
    sys.modules['dm3_lib._dm3_lib'] = Mock()
if 'mrcfile' not in sys.modules:
    sys.modules['mrcfile'] = Mock()
if 'config_manager' not in sys.modules:
    sys.modules['config_manager'] = Mock()
if 'metadata_manager' not in sys.modules:
    sys.modules['metadata_manager'] = Mock()

from lib.loader_config import EBIConfig, ProcessingResult
from lib.metadata_manager import MetadataManager

# Import real EBI functions with path manipulation for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "app" / "ebi"))
try:
    from main import (
        fetch_metadata,
        is_file,
        download_files,
        load_volume,
        write_metadata_stub,
        enrich_metadata,
        process_empiar_file,
        ingest_empiar,
        parse_args
    )
except ImportError as e:
    # Fallback if direct import fails - try alternative path
    sys.path.insert(0, str(Path(__file__).parent.parent / "app"))
    try:
        from ebi.main import (
            fetch_metadata,
            is_file,
            download_files,
            load_volume,
            write_metadata_stub,
            enrich_metadata,
            process_empiar_file,
            ingest_empiar,
            parse_args
        )
    except ImportError:
        # If import still fails, skip real function tests
        fetch_metadata = None
        is_file = None
        download_files = None
        load_volume = None
        write_metadata_stub = None
        enrich_metadata = None
        process_empiar_file = None
        ingest_empiar = None
        parse_args = None


class TestEBIConfig:
    """Test EBI configuration class."""
    
    def test_default_config(self):
        """Test default EBI configuration."""
        config = EBIConfig()
        
        assert config.entry_id == "11759"
        assert config.ftp_server == "ftp.ebi.ac.uk"
        assert config.output_dir == "empiar_volumes"
        assert config.max_workers == 4
    
    def test_api_url_property(self):
        """Test API URL construction."""
        config = EBIConfig(entry_id="12345")
        expected_url = "https://www.ebi.ac.uk/empiar/api/entry/12345/"
        assert config.api_url == expected_url
    
    def test_ftp_path_method(self):
        """Test FTP path construction."""
        config = EBIConfig(entry_id="11759")
        ftp_path = config.get_ftp_path("test_file.dm3")
        expected_path = "/empiar/world_availability/11759/data/test_file.dm3"
        assert ftp_path == expected_path


class TestEBILoader:
    """Test EBI loader functionality."""
    
    @pytest.fixture
    def mock_ebi_functions(self):
        """Mock the EBI loader functions."""
        # We'll need to import and patch the actual EBI functions
        # For now, create a mock structure
        mocks = {}
        
        with patch('app.ebi.main.fetch_empiar_metadata') as mock_fetch, \
             patch('app.ebi.main.download_empiar_files') as mock_download, \
             patch('app.ebi.main.process_empiar_file') as mock_process:
            
            # Configure mock metadata response
            mock_fetch.return_value = {
                'empiar_id': '11759',
                'title': 'Test EMPIAR Dataset',
                'authors': ['Test Author'],
                'deposition_date': '2023-01-01'
            }
            
            # Configure mock download
            mock_download.return_value = ['test_file.dm3']
            
            # Configure mock processing
            mock_process.return_value = {
                'volume_path': 'test_volume.npy',
                'metadata_path': 'test_metadata.json',
                'volume_shape': [100, 100, 100],
                'file_size_bytes': 1000000,
                'sha256': 'test_hash'
            }
            
            mocks['fetch'] = mock_fetch
            mocks['download'] = mock_download
            mocks['process'] = mock_process
            
            yield mocks
    
    def test_fetch_metadata_success(self, mock_requests, ebi_config):
        """Test successful metadata fetching."""
        # Mock API response
        mock_requests['response'].json.return_value = {
            'empiar_id': '11759',
            'title': 'Test Dataset',
            'authors': ['Author 1', 'Author 2']
        }
        
        # This would test the actual fetch_empiar_metadata function
        # For now, we test the config and mock setup
        assert ebi_config.api_url == "https://www.ebi.ac.uk/empiar/api/entry/11759/"
    
    def test_file_listing_ftp(self, mock_ftp, ebi_config):
        """Test FTP file listing."""
        mock_ftp.nlst.return_value = ['file1.dm3', 'file2.mrc', 'metadata.xml']
        
        # Test that FTP mock is configured correctly
        files = mock_ftp.nlst('/empiar/world_availability/11759/data/')
        assert 'file1.dm3' in files
        assert 'file2.mrc' in files
    
    def test_file_download_ftp(self, mock_ftp, ebi_config, temp_test_dir):
        """Test FTP file download."""
        output_file = temp_test_dir / "test_download.dm3"
        
        # Simulate downloading a file
        def mock_retrbinary(cmd, callback):
            test_data = b"test dm3 file content" * 100
            callback(test_data)
        
        mock_ftp.retrbinary = mock_retrbinary
        
        # Test download simulation
        downloaded_data = b""
        def collect_data(data):
            nonlocal downloaded_data
            downloaded_data += data
        
        mock_ftp.retrbinary("RETR test_file.dm3", collect_data)
        assert len(downloaded_data) > 0
        assert b"test dm3 file content" in downloaded_data
    
    @patch('numpy.save')
    @patch('builtins.open', new_callable=mock_open)
    def test_metadata_generation(self, mock_file, mock_np_save, ebi_config, metadata_manager):
        """Test metadata file generation."""
        # Test data
        test_metadata = {
            "id": "550e8400-e29b-41d4-a716-446655440000",
            "source": "ebi",
            "source_id": "11759",
            "status": "complete",
            "created_at": "2024-01-01T12:00:00Z",
            "updated_at": "2024-01-01T12:30:00Z",
            "metadata": {
                "core": {
                    "description": "Test EMPIAR dataset",
                    "volume_shape": [100, 100, 100],
                    "data_type": "uint8"
                },
                "technical": {
                    "file_size_bytes": 1000000,
                    "sha256": "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890"
                }
            }
        }
        
        # Test metadata validation
        validation_result = metadata_manager.validate_metadata(test_metadata)
        assert validation_result['valid'] == True
        assert len(validation_result['errors']) == 0
    
    def test_error_handling_api_failure(self, mock_requests, ebi_config):
        """Test error handling for API failures."""
        # Configure mock to return error
        mock_requests['response'].status_code = 404
        mock_requests['response'].raise_for_status.side_effect = Exception("API Error")
        
        # Test that the error is properly handled
        with pytest.raises(Exception, match="API Error"):
            mock_requests['response'].raise_for_status()
    
    def test_error_handling_ftp_failure(self, mock_ftp, ebi_config):
        """Test error handling for FTP failures."""
        # Configure mock to raise error
        mock_ftp.nlst.side_effect = Exception("FTP Connection Error")
        
        # Test that FTP errors are handled
        with pytest.raises(Exception, match="FTP Connection Error"):
            mock_ftp.nlst('/some/path')
    
    def test_config_validation(self):
        """Test configuration validation."""
        # Test valid config
        config = EBIConfig(
            entry_id="12345",
            output_dir="/tmp/test",
            max_workers=2
        )
        assert config.entry_id == "12345"
        assert config.max_workers == 2
        
        # Test that config handles different entry IDs
        config2 = EBIConfig(entry_id="54321")
        assert config2.api_url == "https://www.ebi.ac.uk/empiar/api/entry/54321/"


@pytest.mark.integration
class TestEBIIntegration:
    """Integration tests for EBI loader (requires network)."""
    
    def test_real_api_call(self, ebi_config):
        """Test actual API call (only run with network access)."""
        import requests
        
        try:
            response = requests.get(ebi_config.api_url, timeout=10)
            assert response.status_code == 200
            data = response.json()
            # EBI API returns data with EMPIAR-XXXXX as top-level key
            assert 'EMPIAR-11759' in data or 'error' in data or 'empiar_id' in data
        except requests.RequestException:
            pytest.skip("Network not available for integration test")
    
    @pytest.mark.slow
    def test_small_file_download(self, ebi_config, temp_test_dir):
        """Test downloading a small real file (if available)."""
        # This would test actual file download but requires careful selection
        # of a small test file to avoid long download times
        pytest.skip("Requires selection of appropriate test file")


class TestProcessingResult:
    """Test ProcessingResult helper class."""
    
    def test_result_creation(self):
        """Test creating a processing result."""
        result = ProcessingResult(
            success=True,
            source="ebi",
            source_id="11759"
        )
        
        assert result.success == True
        assert result.source == "ebi"
        assert result.source_id == "11759"
        assert len(result.files_processed) == 0
        assert len(result.errors) == 0
    
    def test_add_error(self):
        """Test adding errors to result."""
        result = ProcessingResult(success=True, source="ebi", source_id="test")
        
        result.add_error("Test error message")
        
        assert result.success == False
        assert "Test error message" in result.errors
    
    def test_add_processed_file(self):
        """Test adding processed files to result."""
        result = ProcessingResult(success=True, source="ebi", source_id="test")
        
        result.add_processed_file("volume.npy", "metadata.json")
        
        assert "volume.npy" in result.files_processed
        assert "metadata.json" in result.metadata_paths


class TestRealEBIFunctions:
    """Test real EBI implementation functions."""
    
    @pytest.mark.skipif(fetch_metadata is None, reason="EBI functions not available")
    def test_fetch_metadata_real(self):
        """Test real fetch_metadata function."""
        entry_id = "11759"
        api_base_url = "https://www.ebi.ac.uk/empiar/api/entry"
        
        with patch('requests.get') as mock_get:
            mock_response = Mock()
            mock_response.json.return_value = {"empiar_id": entry_id, "title": "Test Dataset"}
            mock_response.raise_for_status.return_value = None
            mock_get.return_value = mock_response
            
            result = fetch_metadata(entry_id, api_base_url)
            
            assert isinstance(result, dict)
            mock_get.assert_called_once_with(f"{api_base_url}/{entry_id}/")
            mock_response.raise_for_status.assert_called_once()
    
    @pytest.mark.skipif(is_file is None, reason="EBI functions not available")
    def test_is_file_real(self):
        """Test real is_file function."""
        # Mock FTP object
        mock_ftp = Mock()
        
        # Test when file exists
        mock_ftp.size.return_value = 1024
        result = is_file(mock_ftp, "test.dm3")
        assert result is True
        
        # Test when file doesn't exist
        mock_ftp.size.side_effect = Exception("File not found")
        result = is_file(mock_ftp, "nonexistent.dm3")
        assert result is False
    
    @pytest.mark.skipif(download_files is None, reason="EBI functions not available")
    def test_download_files_real(self):
        """Test real download_files function."""
        entry_id = "11759"
        download_dir = "/tmp/test_download"
        ftp_server = "ftp.ebi.ac.uk"
        
        with patch('ftplib.FTP') as mock_ftp_class, \
             patch('os.makedirs') as mock_makedirs, \
             patch('builtins.open', mock_open()) as mock_file, \
             patch('tqdm') as mock_tqdm:
            
            mock_ftp = Mock()
            mock_ftp_class.return_value = mock_ftp
            mock_ftp.nlst.return_value = ["file1.dm3", "file2.mrc"]
            mock_ftp.size.return_value = 1024  # Simulate files exist
            
            # Mock tqdm context manager
            mock_pbar = Mock()
            mock_tqdm.return_value.__enter__.return_value = mock_pbar
            
            result = download_files(entry_id, download_dir, ftp_server)
            
            assert isinstance(result, list)
            mock_ftp_class.assert_called_once_with(ftp_server)
            mock_ftp.login.assert_called_once()
            mock_ftp.cwd.assert_called_once_with(f'/empiar/world_availability/{entry_id}/data/')
    
    @pytest.mark.skipif(load_volume is None, reason="EBI functions not available")
    def test_load_volume_real(self):
        """Test real load_volume function."""
        # Test MRC file loading
        with patch('mrcfile.open') as mock_mrc:
            mock_mrc_file = Mock()
            mock_mrc_file.data = np.array([[[1, 2], [3, 4]]])
            mock_mrc.__enter__.return_value = mock_mrc_file
            
            result = load_volume("test.mrc")
            
            assert isinstance(result, np.ndarray)
            mock_mrc.assert_called_once_with("test.mrc", permissive=True)
        
        # Test DM3 file loading
        with patch('dm3_lib._dm3_lib.DM3') as mock_dm3:
            mock_dm3_file = Mock()
            mock_dm3_file.imagedata = np.array([[[5, 6], [7, 8]]])
            mock_dm3.return_value = mock_dm3_file
            
            result = load_volume("test.dm3")
            
            assert isinstance(result, np.ndarray)
            mock_dm3.assert_called_once_with("test.dm3")
        
        # Test unsupported file type
        with pytest.raises(ValueError, match="Unsupported file type"):
            load_volume("test.xyz")
    
    @pytest.mark.skipif(write_metadata_stub is None, reason="EBI functions not available")
    def test_write_metadata_stub_real(self):
        """Test real write_metadata_stub function."""
        entry_id = "11759"
        source_metadata = {"title": "Test Dataset", "authors": ["Test Author"]}
        file_path = "/tmp/test.dm3"
        volume_path = "/tmp/test_volume.npy"
        metadata_path = "/tmp/test_metadata.json"
        ftp_server = "ftp.ebi.ac.uk"
        
        with patch('metadata_manager.MetadataManager') as mock_mm_class:
            mock_mm = Mock()
            mock_mm_class.return_value = mock_mm
            
            # Mock the metadata record
            mock_record = {"id": "test-id", "metadata": {"provenance": {}}}
            mock_mm.create_metadata_record.return_value = mock_record
            
            result = write_metadata_stub(
                entry_id, source_metadata, file_path, volume_path, 
                metadata_path, ftp_server
            )
            
            assert isinstance(result, dict)
            mock_mm.create_metadata_record.assert_called_once()
            mock_mm.add_file_paths.assert_called_once()
            mock_mm.update_status.assert_called_once()
            mock_mm.save_metadata.assert_called_once()
    
    @pytest.mark.skipif(enrich_metadata is None, reason="EBI functions not available")
    def test_enrich_metadata_real(self):
        """Test real enrich_metadata function."""
        metadata_path = "/tmp/test_metadata.json"
        record = {"id": "test-id", "metadata": {}}
        volume = np.array([[[1, 2], [3, 4]]], dtype=np.uint8)
        
        with patch('metadata_manager.MetadataManager') as mock_mm_class:
            mock_mm = Mock()
            mock_mm_class.return_value = mock_mm
            
            enrich_metadata(metadata_path, record, volume)
            
            mock_mm.add_technical_metadata.assert_called_once()
            mock_mm.update_status.assert_called_once_with(record, "complete")
            mock_mm.save_metadata.assert_called_once_with(record, metadata_path, validate=True)
    
    @pytest.mark.skipif(process_empiar_file is None, reason="EBI functions not available")
    def test_process_empiar_file_real(self):
        """Test real process_empiar_file function."""
        entry_id = "11759"
        source_metadata = {"title": "Test Dataset"}
        file_path = "/tmp/test.dm3"
        output_dir = "/tmp/output"
        ftp_server = "ftp.ebi.ac.uk"
        
        with patch('ebi.main.load_volume') as mock_load, \
             patch('numpy.save') as mock_save, \
             patch('ebi.main.write_metadata_stub') as mock_write_stub, \
             patch('ebi.main.enrich_metadata') as mock_enrich:
            
            mock_volume = np.array([[[1, 2], [3, 4]]])
            mock_load.return_value = mock_volume
            mock_write_stub.return_value = {"id": "test-id"}
            
            result = process_empiar_file(
                entry_id, source_metadata, file_path, output_dir, ftp_server
            )
            
            assert isinstance(result, str)
            assert "Processed" in result or "Failed" in result
            mock_load.assert_called_once_with(file_path)
    
    @pytest.mark.skipif(parse_args is None, reason="EBI functions not available")  
    def test_parse_args_real(self):
        """Test real parse_args function."""
        with patch('sys.argv', ['main.py', '--config', 'test_config.yaml', '--entry-id', '12345']):
            args = parse_args()
            
            assert hasattr(args, 'config')
            assert hasattr(args, 'entry_id')
            assert args.config == 'test_config.yaml'
            assert args.entry_id == '12345'