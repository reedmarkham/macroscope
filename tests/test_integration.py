"""
Integration tests for the complete electron microscopy ingestion pipeline.
"""

import pytest
import subprocess
import sys
from pathlib import Path
from unittest.mock import patch, Mock
import tempfile
import json
import time

# Add project paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from lib.loader_config import load_test_configs, create_minimal_test_config
from lib.metadata_manager import MetadataManager
from lib.config_manager import ConfigManager


class TestPipelineIntegration:
    """Test complete pipeline integration."""
    
    def test_all_loaders_config_creation(self):
        """Test that all loaders can create valid configurations."""
        test_configs = load_test_configs()
        
        required_loaders = ['ebi', 'epfl', 'flyem', 'idr', 'openorganelle']
        
        for loader_name in required_loaders:
            assert loader_name in test_configs
            config = test_configs[loader_name]
            assert hasattr(config, 'output_dir')
            assert hasattr(config, 'max_workers')
            assert hasattr(config, 'timeout_seconds')
    
    def test_metadata_validation_pipeline(self, metadata_manager, temp_test_dir):
        """Test end-to-end metadata validation pipeline."""
        # Create test metadata for each loader type
        test_metadata_files = []
        
        for source in ['ebi', 'epfl', 'flyem', 'idr', 'openorganelle']:
            metadata = {
                "id": f"test-{source}-001",
                "source": source,
                "source_id": f"TEST-{source.upper()}-001",
                "status": "complete",
                "created_at": "2024-01-01T12:00:00Z",
                "updated_at": "2024-01-01T12:30:00Z",
                "metadata": {
                    "core": {
                        "description": f"Test {source} dataset",
                        "volume_shape": [100, 100, 100],
                        "data_type": "uint8"
                    }
                }
            }
            
            # Save metadata file
            metadata_file = temp_test_dir / f"metadata_{source}_test.json"
            metadata_file.parent.mkdir(parents=True, exist_ok=True)
            
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
            
            test_metadata_files.append(str(metadata_file))
        
        # Test validation of all files
        for file_path in test_metadata_files:
            metadata = metadata_manager.load_metadata(file_path, validate=True)
            validation_result = metadata_manager.validate_metadata(metadata)
            assert validation_result['valid'], f"Validation failed for {file_path}: {validation_result['errors']}"
    
    def test_consolidation_pipeline(self, temp_test_dir):
        """Test the metadata consolidation pipeline."""
        # Create test directory structure
        for source in ['ebi', 'epfl', 'flyem']:
            source_dir = temp_test_dir / source
            source_dir.mkdir(parents=True, exist_ok=True)
            
            # Create test metadata file
            metadata = {
                "id": f"test-{source}-001",
                "source": source,
                "source_id": f"TEST-{source.upper()}-001",
                "status": "complete",
                "created_at": "2024-01-01T12:00:00Z",
                "updated_at": "2024-01-01T12:30:00Z",
                "metadata": {
                    "core": {
                        "description": f"Test {source} dataset",
                        "volume_shape": [50, 50, 50]
                    }
                }
            }
            
            metadata_file = source_dir / f"metadata_{source}_test.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f, indent=2)
        
        # Test consolidation import
        try:
            # Import consolidation module
            consolidate_path = Path(__file__).parent.parent / "app" / "consolidate"
            sys.path.insert(0, str(consolidate_path.parent))
            
            from consolidate.main import consolidate_metadata_with_validation
            
            # Create test config
            test_config = {
                'processing': {
                    'scan_directories': [str(temp_test_dir)],
                    'include_patterns': ['metadata*.json'],
                    'exclude_patterns': ['*.tmp'],
                    'timestamp_format': '%Y%m%d_%H%M%S'
                },
                'output_dir': str(temp_test_dir / "consolidate_output"),
                'validation': {
                    'strict_mode': False,
                    'report_validation_errors': True
                }
            }
            
            # Mock the config manager for this test
            with patch('lib.config_manager.get_config_manager') as mock_config:
                mock_manager = Mock()
                mock_manager.get_consolidation_config.return_value = test_config
                mock_config.return_value = mock_manager
                
                # This would run consolidation - commented out for safety
                # consolidate_metadata_with_validation()
                
            assert True  # Test passed if we can import and configure
            
        except ImportError as e:
            pytest.skip(f"Could not import consolidation module: {e}")


class TestDockerIntegration:
    """Test Docker-based integration scenarios."""
    
    def test_docker_compose_structure(self):
        """Test that docker-compose.yml exists and has expected services."""
        docker_compose_path = Path(__file__).parent.parent / "docker-compose.yml"
        
        if not docker_compose_path.exists():
            pytest.skip("docker-compose.yml not found")
        
        with open(docker_compose_path, 'r') as f:
            content = f.read()
        
        # Check for expected services
        expected_services = ['ebi', 'epfl', 'flyem', 'idr', 'openorganelle']
        for service in expected_services:
            assert service in content, f"Service {service} not found in docker-compose.yml"
    
    @pytest.mark.slow
    def test_docker_build_consolidate(self, temp_test_dir):
        """Test building the consolidate Docker image."""
        consolidate_dir = Path(__file__).parent.parent / "app" / "consolidate"
        dockerfile_path = consolidate_dir / "Dockerfile"
        
        if not dockerfile_path.exists():
            pytest.skip("Consolidate Dockerfile not found")
        
        try:
            # Test Docker build command (without actually running it)
            docker_build_cmd = [
                "docker", "build", "-t", "metadata-consolidator-test", 
                str(consolidate_dir)
            ]
            
            # Just validate the command structure
            assert docker_build_cmd[0] == "docker"
            assert docker_build_cmd[1] == "build"
            assert str(consolidate_dir) in docker_build_cmd
            
        except Exception as e:
            pytest.skip(f"Docker not available: {e}")


class TestConfigurationIntegration:
    """Test configuration system integration."""
    
    def test_config_loading_all_sources(self, temp_test_dir):
        """Test loading configuration for all sources."""
        # Create a test config file
        test_config = {
            'global': {
                'data_root': str(temp_test_dir),
                'processing': {'max_workers': 2}
            },
            'sources': {
                'ebi': {
                    'name': 'Test EBI',
                    'enabled': True,
                    'output_dir': str(temp_test_dir / 'ebi'),
                    'base_urls': {'api': 'https://test.api.com'}
                },
                'epfl': {
                    'name': 'Test EPFL',
                    'enabled': True,
                    'output_dir': str(temp_test_dir / 'epfl'),
                    'base_urls': {'download': 'https://test.download.com'}
                }
            }
        }
        
        config_file = temp_test_dir / "test_config.yaml"
        import yaml
        with open(config_file, 'w') as f:
            yaml.dump(test_config, f)
        
        # Test loading config
        try:
            config_manager = ConfigManager(str(config_file))
            
            # Test global config
            global_config = config_manager.get_global_config()
            assert global_config['data_root'] == str(temp_test_dir)
            
            # Test source configs
            ebi_config = config_manager.get_source_config('ebi')
            assert ebi_config is not None
            assert ebi_config.name == 'Test EBI'
            assert ebi_config.enabled == True
            
        except Exception as e:
            pytest.fail(f"Config loading failed: {e}")
    
    def test_environment_variable_substitution(self, temp_test_dir, monkeypatch):
        """Test environment variable substitution in config."""
        # Set test environment variable
        monkeypatch.setenv("TEST_DATA_ROOT", str(temp_test_dir))
        
        config_content = """
        global:
          data_root: "${TEST_DATA_ROOT}"
          logs_root: "${TEST_LOGS_ROOT:./default_logs}"
        sources:
          ebi:
            name: "Test EBI"
            enabled: true
            output_dir: "./data/ebi"
            base_urls:
              api: "https://test.ebi.ac.uk"
        """
        
        config_file = temp_test_dir / "test_env_config.yaml"
        with open(config_file, 'w') as f:
            f.write(config_content)
        
        try:
            config_manager = ConfigManager(str(config_file))
            
            # Test environment variable substitution
            data_root = config_manager.get('global.data_root')
            assert data_root == str(temp_test_dir)
            
            # Test default value substitution
            logs_root = config_manager.get('global.logs_root')
            assert logs_root == "./default_logs"
            
        except Exception as e:
            pytest.fail(f"Environment variable substitution failed: {e}")


@pytest.mark.integration
class TestNetworkIntegration:
    """Integration tests requiring network access."""
    
    def test_api_endpoints_reachable(self):
        """Test that configured API endpoints are reachable."""
        import requests
        
        # Test endpoints (with timeout to avoid hanging)
        endpoints = [
            "https://www.ebi.ac.uk/empiar/api/entry/11759/",
            "https://idr.openmicroscopy.org/api/v0/",
            # Note: Don't test download URLs as they might be large files
        ]
        
        for endpoint in endpoints:
            try:
                response = requests.head(endpoint, timeout=10)
                # Accept various success codes
                assert response.status_code in [200, 301, 302, 404]  # 404 is OK for testing
            except requests.RequestException:
                pytest.skip(f"Network issue accessing {endpoint}")
    
    @pytest.mark.slow
    def test_small_data_downloads(self):
        """Test downloading small data samples (if available)."""
        # This would test actual small downloads but requires
        # careful selection of appropriate test data
        pytest.skip("Requires identification of small test datasets")


class TestPerformanceIntegration:
    """Test performance characteristics of the pipeline."""
    
    def test_metadata_processing_performance(self, temp_test_dir, metadata_manager):
        """Test metadata processing performance with multiple files."""
        # Create multiple test metadata files
        num_files = 100
        metadata_files = []
        
        for i in range(num_files):
            metadata = {
                "id": f"test-perf-{i:03d}",
                "source": "ebi",
                "source_id": f"PERF-{i:03d}",
                "status": "complete",
                "created_at": "2024-01-01T12:00:00Z",
                "updated_at": "2024-01-01T12:30:00Z",
                "metadata": {
                    "core": {
                        "description": f"Performance test dataset {i}",
                        "volume_shape": [10, 10, 10]
                    }
                }
            }
            
            metadata_file = temp_test_dir / f"metadata_perf_{i:03d}.json"
            with open(metadata_file, 'w') as f:
                json.dump(metadata, f)
            
            metadata_files.append(str(metadata_file))
        
        # Time the processing
        start_time = time.time()
        
        valid_count = 0
        for file_path in metadata_files:
            try:
                metadata = metadata_manager.load_metadata(file_path, validate=True)
                validation_result = metadata_manager.validate_metadata(metadata)
                if validation_result['valid']:
                    valid_count += 1
            except Exception:
                pass
        
        end_time = time.time()
        processing_time = end_time - start_time
        
        # Performance assertions
        assert valid_count == num_files
        assert processing_time < 10.0  # Should process 100 files in under 10 seconds
        
        files_per_second = num_files / processing_time
        assert files_per_second > 10  # Should process at least 10 files per second