"""
Comprehensive unit tests for the MetadataManager class.

Tests all core functionality including metadata creation, validation,
status tracking, technical metadata management, and file operations.
"""

import pytest
import json
import tempfile
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import Mock, patch, mock_open
from typing import Dict, Any

import sys
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from lib.metadata_manager import MetadataManager, ProcessingStatus
import jsonschema


# Global fixtures for all test classes
@pytest.fixture(scope="session")
def temp_schema_file():
    """Create a temporary schema file for testing."""
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "type": "object",
        "required": ["id", "source", "source_id", "status", "created_at", "metadata"],
        "properties": {
            "id": {"type": "string"},
            "source": {"type": "string", "enum": ["ebi", "epfl", "flyem", "idr", "openorganelle"]},
            "source_id": {"type": "string"},
            "status": {"type": "string", "enum": ["pending", "processing", "saving-data", "complete", "failed", "cancelled"]},
            "created_at": {"type": "string", "format": "date-time"},
            "updated_at": {"type": "string", "format": "date-time"},
            "metadata": {"type": "object"}
        }
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(schema, f)
        f.flush()  # Ensure data is written
        file_path = f.name
    
    yield file_path
    
    # Cleanup
    try:
        os.unlink(file_path)
    except FileNotFoundError:
        pass

@pytest.fixture
def test_metadata_manager(temp_schema_file: str) -> MetadataManager:
    """Create MetadataManager instance with test schema."""
    return MetadataManager(schema_path=temp_schema_file)

@pytest.fixture
def temp_dir() -> str:
    """Create temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir

@pytest.fixture
def sample_record(test_metadata_manager: MetadataManager) -> Dict[str, Any]:
    """Create a sample metadata record for testing."""
    return test_metadata_manager.create_metadata_record(
        source="ebi",
        source_id="test",
        description="Test record"
    )


class TestMetadataManager:
    """Test cases for MetadataManager class."""

    def test_init_with_default_schema(self):
        """Test MetadataManager initialization with default schema path."""
        with patch('builtins.open', mock_open(read_data='{"type": "object"}')), \
             patch('jsonschema.Draft7Validator.check_schema'):
            manager = MetadataManager()
            assert manager.schema_path.name == "metadata_schema.json"

    def test_init_with_custom_schema(self, test_metadata_manager: MetadataManager, temp_schema_file: str):
        """Test MetadataManager initialization with custom schema."""
        assert str(test_metadata_manager.schema_path) == temp_schema_file
        assert test_metadata_manager.schema is not None

    def test_init_with_invalid_schema_path(self):
        """Test MetadataManager initialization with invalid schema path."""
        with pytest.raises(FileNotFoundError):
            MetadataManager(schema_path="/nonexistent/schema.json")

    def test_init_with_invalid_schema_format(self):
        """Test MetadataManager initialization with malformed schema."""
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            f.write('{"invalid": "schema", "properties": {"id": "not-an-object"}}')
            schema_path = f.name
        
        try:
            with pytest.raises(jsonschema.SchemaError):
                MetadataManager(schema_path=schema_path)
        finally:
            os.unlink(schema_path)


class TestCreateMetadataRecord:
    """Test metadata record creation functionality."""

    def test_create_basic_metadata_record(self, test_metadata_manager: MetadataManager):
        """Test creating a basic metadata record."""
        record = test_metadata_manager.create_metadata_record(
            source="ebi",
            source_id="EMPIAR-11759",
            description="Test dataset"
        )
        
        assert record["source"] == "ebi"
        assert record["source_id"] == "EMPIAR-11759"
        assert record["status"] == ProcessingStatus.PENDING.value
        assert record["metadata"]["core"]["description"] == "Test dataset"
        assert "id" in record
        assert "created_at" in record
        assert "updated_at" in record

    def test_create_metadata_record_with_custom_id(self, test_metadata_manager: MetadataManager):
        """Test creating metadata record with custom ID."""
        custom_id = "test-custom-id-123"
        record = test_metadata_manager.create_metadata_record(
            source="flyem",
            source_id="hemibrain-v1.2",
            description="Test with custom ID",
            record_id=custom_id
        )
        
        assert record["id"] == custom_id

    def test_create_metadata_record_with_additional_metadata(self, test_metadata_manager: MetadataManager):
        """Test creating metadata record with additional metadata."""
        additional = {
            "metadata": {
                "core": {
                    "volume_shape": [100, 100, 100],
                    "data_type": "uint8"
                },
                "technical": {
                    "file_size_bytes": 1024000
                }
            }
        }
        
        record = test_metadata_manager.create_metadata_record(
            source="openorganelle",
            source_id="jrc_cos7_1",
            description="Test with additional metadata",
            initial_metadata=additional
        )
        
        assert record["metadata"]["core"]["volume_shape"] == [100, 100, 100]
        assert record["metadata"]["core"]["data_type"] == "uint8"
        assert record["metadata"]["technical"]["file_size_bytes"] == 1024000

    @patch.dict(os.environ, {"EM_CI_METADATA": "true", "CI_COMMIT_SHA": "abc123"})
    def test_create_metadata_record_with_ci_metadata(self, test_metadata_manager: MetadataManager):
        """Test creating metadata record with CI metadata."""
        record = test_metadata_manager.create_metadata_record(
            source="idr",
            source_id="idr0086",
            description="Test with CI metadata",
            include_ci_metadata=True
        )
        
        assert "ci_metadata" in record
        assert record["ci_metadata"]["commit_sha"] == "abc123"

    def test_create_metadata_record_without_ci_metadata(self, test_metadata_manager: MetadataManager):
        """Test creating metadata record without CI metadata."""
        record = test_metadata_manager.create_metadata_record(
            source="epfl",
            source_id="hippocampus",
            description="Test without CI metadata",
            include_ci_metadata=False
        )
        
        assert "ci_metadata" not in record


class TestStatusUpdate:
    """Test status update functionality."""



    def test_update_status_with_enum(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test updating status using ProcessingStatus enum."""
        original_updated_at = sample_record["updated_at"]
        
        updated = test_metadata_manager.update_status(
            sample_record, 
            ProcessingStatus.PROCESSING
        )
        
        assert updated["status"] == "processing"
        assert "updated_at" in updated
        # Just check that updated_at field exists, not that it's different
        # since the timestamp might be the same if executed quickly

    def test_update_status_with_string(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test updating status using string value."""
        updated = test_metadata_manager.update_status(
            sample_record,
            "complete"
        )
        
        assert updated["status"] == "complete"

    def test_update_status_with_progress(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test updating status with progress information."""
        updated = test_metadata_manager.update_status(
            sample_record,
            ProcessingStatus.PROCESSING,
            progress_percentage=50.0,
            current_step="Processing volume data"
        )
        
        assert updated["progress"]["percentage"] == 50.0
        assert updated["progress"]["current_step"] == "Processing volume data"

    def test_update_status_to_failed_with_error(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test updating status to failed with error information."""
        updated = test_metadata_manager.update_status(
            sample_record,
            ProcessingStatus.FAILED,
            error_message="Download failed",
            error_traceback="Traceback: ConnectionError..."
        )
        
        assert updated["status"] == "failed"
        assert updated["error"]["message"] == "Download failed"
        assert updated["error"]["traceback"] == "Traceback: ConnectionError..."
        assert updated["error"]["retry_count"] == 1

    def test_update_status_failed_multiple_times(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test retry count increment on multiple failures."""
        # First failure
        updated = test_metadata_manager.update_status(
            sample_record,
            ProcessingStatus.FAILED,
            error_message="First failure"
        )
        assert updated["error"]["retry_count"] == 1
        
        # Second failure
        updated = test_metadata_manager.update_status(
            updated,
            ProcessingStatus.FAILED,
            error_message="Second failure"
        )
        assert updated["error"]["retry_count"] == 2


class TestTechnicalMetadata:
    """Test technical metadata management."""



    def test_add_technical_metadata_core_fields(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test adding core technical metadata fields."""
        updated = test_metadata_manager.add_technical_metadata(
            sample_record,
            volume_shape=[512, 512, 256],
            voxel_size_nm={"x": 4.0, "y": 4.0, "z": 40.0},
            data_type="uint8"
        )
        
        assert updated["metadata"]["core"]["volume_shape"] == [512, 512, 256]
        assert updated["metadata"]["core"]["voxel_size_nm"] == {"x": 4.0, "y": 4.0, "z": 40.0}
        assert updated["metadata"]["core"]["data_type"] == "uint8"

    def test_add_technical_metadata_file_info(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test adding file information metadata."""
        updated = test_metadata_manager.add_technical_metadata(
            sample_record,
            file_size_bytes=1024000,
            sha256="abc123def456"
        )
        
        assert updated["metadata"]["technical"]["file_size_bytes"] == 1024000
        assert updated["metadata"]["technical"]["sha256"] == "abc123def456"

    def test_add_technical_metadata_custom_fields(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test adding custom technical metadata fields."""
        updated = test_metadata_manager.add_technical_metadata(
            sample_record,
            compression="gzip",
            chunk_size=[64, 64, 64],
            custom_field="custom_value"
        )
        
        assert updated["metadata"]["technical"]["compression"] == "gzip"
        assert updated["metadata"]["technical"]["chunk_size"] == [64, 64, 64]
        assert updated["metadata"]["technical"]["custom_field"] == "custom_value"

    def test_add_technical_metadata_initializes_structure(self, test_metadata_manager: MetadataManager):
        """Test that technical metadata initializes missing structure."""
        record = {"id": "test"}  # Minimal record
        
        updated = test_metadata_manager.add_technical_metadata(
            record,
            volume_shape=[100, 100, 100]
        )
        
        assert "metadata" in updated
        assert "core" in updated["metadata"]
        assert "technical" in updated["metadata"]
        assert updated["metadata"]["core"]["volume_shape"] == [100, 100, 100]


class TestFilePathManagement:
    """Test file path management functionality."""



    def test_add_file_paths_basic(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test adding basic file paths."""
        updated = test_metadata_manager.add_file_paths(
            sample_record,
            volume_path="/data/volume.zarr",
            raw_path="/data/raw.dm3",
            metadata_path="/data/metadata.json"
        )
        
        assert updated["files"]["volume"] == "/data/volume.zarr"
        assert updated["files"]["raw"] == "/data/raw.dm3"
        assert updated["files"]["metadata"] == "/data/metadata.json"

    def test_add_file_paths_additional(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test adding additional file paths."""
        updated = test_metadata_manager.add_file_paths(
            sample_record,
            thumbnail="/data/thumb.png",
            log_file="/logs/process.log"
        )
        
        assert updated["files"]["additional"]["thumbnail"] == "/data/thumb.png"
        assert updated["files"]["additional"]["log_file"] == "/logs/process.log"

    def test_add_file_paths_partial_update(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any]):
        """Test partial file path updates."""
        # First update
        updated = test_metadata_manager.add_file_paths(
            sample_record,
            volume_path="/data/volume.zarr"
        )
        
        # Second update adds more paths
        updated = test_metadata_manager.add_file_paths(
            updated,
            raw_path="/data/raw.dm3"
        )
        
        assert updated["files"]["volume"] == "/data/volume.zarr"
        assert updated["files"]["raw"] == "/data/raw.dm3"


class TestValidation:
    """Test metadata validation functionality."""


    def test_validate_valid_metadata(self, test_metadata_manager: MetadataManager):
        """Test validation of valid metadata."""
        record = test_metadata_manager.create_metadata_record(
            source="ebi",
            source_id="test",
            description="Valid record"
        )
        
        result = test_metadata_manager.validate_metadata(record)
        assert result["valid"] is True
        assert result["errors"] == []

    def test_validate_invalid_metadata_missing_required(self, test_metadata_manager: MetadataManager):
        """Test validation of metadata missing required fields."""
        invalid_record = {
            "id": "test",
            "source": "ebi"
            # Missing required fields: source_id, status, created_at, metadata
        }
        
        result = test_metadata_manager.validate_metadata(invalid_record)
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_validate_invalid_metadata_wrong_enum(self, test_metadata_manager: MetadataManager):
        """Test validation of metadata with invalid enum values."""
        record = test_metadata_manager.create_metadata_record(
            source="ebi",
            source_id="test",
            description="Test record"
        )
        record["source"] = "invalid_source"  # Not in enum
        
        result = test_metadata_manager.validate_metadata(record)
        assert result["valid"] is False
        assert len(result["errors"]) > 0

    def test_validate_metadata_exception_handling(self, test_metadata_manager: MetadataManager):
        """Test validation error handling for malformed data."""
        with patch('jsonschema.validate', side_effect=Exception("Validation error")):
            result = test_metadata_manager.validate_metadata({"test": "data"})
            assert result["valid"] is False
            assert "Validation failed" in result["errors"][0]


class TestFileOperations:
    """Test file save/load operations."""



    def test_save_metadata_success(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any], temp_dir: str):
        """Test successful metadata save."""
        output_path = os.path.join(temp_dir, "test_metadata.json")
        
        test_metadata_manager.save_metadata(sample_record, output_path)
        
        assert os.path.exists(output_path)
        with open(output_path, 'r') as f:
            saved_data = json.load(f)
        assert saved_data["id"] == sample_record["id"]

    def test_save_metadata_validation_failure(self, test_metadata_manager: MetadataManager, temp_dir: str):
        """Test save failure due to validation error."""
        invalid_record = {"invalid": "record"}
        output_path = os.path.join(temp_dir, "invalid_metadata.json")
        
        with pytest.raises(ValueError, match="Metadata validation failed"):
            test_metadata_manager.save_metadata(invalid_record, output_path, validate=True)

    def test_save_metadata_skip_validation(self, test_metadata_manager: MetadataManager, temp_dir: str):
        """Test save with validation skipped."""
        invalid_record = {"invalid": "record"}
        output_path = os.path.join(temp_dir, "invalid_metadata.json")
        
        test_metadata_manager.save_metadata(invalid_record, output_path, validate=False)
        assert os.path.exists(output_path)

    def test_save_metadata_atomic_write(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any], temp_dir: str):
        """Test atomic write behavior (temp file cleanup on error)."""
        output_path = os.path.join(temp_dir, "test_metadata.json")
        temp_path = f"{output_path}.tmp"
        
        with patch('builtins.open', side_effect=IOError("Write failed")):
            with pytest.raises(IOError):
                test_metadata_manager.save_metadata(sample_record, output_path, validate=False)
        
        # Temp file should be cleaned up
        assert not os.path.exists(temp_path)

    def test_load_metadata_success(self, test_metadata_manager: MetadataManager, sample_record: Dict[str, Any], temp_dir: str):
        """Test successful metadata load."""
        output_path = os.path.join(temp_dir, "test_metadata.json")
        
        # Save first
        test_metadata_manager.save_metadata(sample_record, output_path)
        
        # Load and verify
        loaded_record = test_metadata_manager.load_metadata(output_path)
        assert loaded_record["id"] == sample_record["id"]
        assert loaded_record["source"] == sample_record["source"]

    def test_load_metadata_validation_warning(self, test_metadata_manager: MetadataManager, temp_dir: str):
        """Test load with validation warning for invalid data."""
        output_path = os.path.join(temp_dir, "invalid_metadata.json")
        invalid_record = {"invalid": "record"}
        
        with open(output_path, 'w') as f:
            json.dump(invalid_record, f)
        
        with patch.object(test_metadata_manager.logger, 'warning') as mock_warning:
            loaded_record = test_metadata_manager.load_metadata(output_path, validate=True)
            mock_warning.assert_called_once()
        
        assert loaded_record == invalid_record

    def test_load_metadata_file_not_found(self, test_metadata_manager: MetadataManager):
        """Test load failure for non-existent file."""
        with pytest.raises(FileNotFoundError):
            test_metadata_manager.load_metadata("/nonexistent/file.json")


class TestProcessingSummary:
    """Test processing summary functionality."""


    def test_get_processing_summary_empty_list(self, test_metadata_manager: MetadataManager):
        """Test processing summary with empty file list."""
        summary = test_metadata_manager.get_processing_summary([])
        
        assert summary["total_files"] == 0
        assert summary["status_counts"] == {}
        assert summary["source_counts"] == {}
        assert summary["validation_errors"] == []
        assert summary["processing_times"] == []

    def test_get_processing_summary_multiple_files(self, test_metadata_manager: MetadataManager, temp_dir: str):
        """Test processing summary with multiple metadata files."""
        # Create test metadata files
        files = []
        for i, (source, status) in enumerate([("ebi", "complete"), ("flyem", "processing"), ("ebi", "failed")]):
            record = test_metadata_manager.create_metadata_record(
                source=source,
                source_id=f"test-{i}",
                description=f"Test record {i}"
            )
            record = test_metadata_manager.update_status(record, status)
            
            file_path = os.path.join(temp_dir, f"metadata_{i}.json")
            test_metadata_manager.save_metadata(record, file_path)
            files.append(file_path)
        
        summary = test_metadata_manager.get_processing_summary(files)
        
        assert summary["total_files"] == 3
        assert summary["status_counts"]["complete"] == 1
        assert summary["status_counts"]["processing"] == 1
        assert summary["status_counts"]["failed"] == 1
        assert summary["source_counts"]["ebi"] == 2
        assert summary["source_counts"]["flyem"] == 1

    def test_get_processing_summary_with_validation_errors(self, test_metadata_manager: MetadataManager, temp_dir: str):
        """Test processing summary with validation errors."""
        # Create invalid metadata file
        invalid_record = {"invalid": "record"}
        file_path = os.path.join(temp_dir, "invalid_metadata.json")
        with open(file_path, 'w') as f:
            json.dump(invalid_record, f)
        
        summary = test_metadata_manager.get_processing_summary([file_path])
        
        assert summary["total_files"] == 1
        assert len(summary["validation_errors"]) == 1
        assert summary["validation_errors"][0]["file"] == file_path

    def test_get_processing_summary_with_processing_times(self, test_metadata_manager: MetadataManager, temp_dir: str):
        """Test processing summary calculates processing times."""
        record = test_metadata_manager.create_metadata_record(
            source="ebi",
            source_id="test",
            description="Test record"
        )
        
        # Manually set timestamps with known difference
        record["created_at"] = "2024-01-01T10:00:00Z"
        record["updated_at"] = "2024-01-01T10:05:00Z"  # 5 minutes later
        
        file_path = os.path.join(temp_dir, "timed_metadata.json")
        test_metadata_manager.save_metadata(record, file_path, validate=False)
        
        summary = test_metadata_manager.get_processing_summary([file_path])
        
        assert len(summary["processing_times"]) == 1
        assert summary["processing_times"][0] == 300.0  # 5 minutes in seconds


class TestCIMetadata:
    """Test CI/CD metadata functionality."""


    @patch.dict(os.environ, {
        "CI_COMMIT_SHA": "abc123",
        "CI_COMMIT_REF": "main",
        "CI_PIPELINE_ID": "12345",
        "RUNNER_OS": "Linux",
        "GITHUB_REPOSITORY": "test/repo"
    })
    def test_get_ci_metadata(self, test_metadata_manager: MetadataManager):
        """Test CI metadata extraction from environment variables."""
        ci_metadata = test_metadata_manager._get_ci_metadata()
        
        assert ci_metadata["commit_sha"] == "abc123"
        assert ci_metadata["commit_ref"] == "main"
        assert ci_metadata["pipeline_id"] == "12345"
        assert ci_metadata["runner_os"] == "Linux"
        assert ci_metadata["github_repository"] == "test/repo"
        assert ci_metadata["processing_environment"] == "github_actions"

    def test_get_ci_metadata_missing_env_vars(self, test_metadata_manager: MetadataManager):
        """Test CI metadata with missing environment variables."""
        ci_metadata = test_metadata_manager._get_ci_metadata()
        
        # Should handle missing env vars gracefully
        assert ci_metadata["commit_sha"] is None
        assert ci_metadata["processing_environment"] == "github_actions"


class TestProcessingStatusEnum:
    """Test ProcessingStatus enum functionality."""

    def test_processing_status_values(self):
        """Test ProcessingStatus enum values."""
        assert ProcessingStatus.PENDING.value == "pending"
        assert ProcessingStatus.PROCESSING.value == "processing"
        assert ProcessingStatus.SAVING_DATA.value == "saving-data"
        assert ProcessingStatus.COMPLETE.value == "complete"
        assert ProcessingStatus.FAILED.value == "failed"
        assert ProcessingStatus.CANCELLED.value == "cancelled"

    def test_processing_status_enum_usage(self):
        """Test using ProcessingStatus enum in comparisons."""
        status = ProcessingStatus.PROCESSING
        assert status == ProcessingStatus.PROCESSING
        assert status.value == "processing"
        assert status != ProcessingStatus.COMPLETE


@pytest.mark.integration
class TestMetadataManagerIntegration:
    """Integration tests for MetadataManager with real schema."""

    def test_with_real_schema(self):
        """Test MetadataManager with the actual project schema."""
        # This test uses the real schema file from the project
        schema_path = Path(__file__).parent.parent / "schemas" / "metadata_schema.json"
        
        if schema_path.exists():
            manager = MetadataManager(schema_path=str(schema_path))
            
            # Create a complete record
            record = manager.create_metadata_record(
                source="ebi",
                source_id="EMPIAR-11759",
                description="Real schema test"
            )
            
            # Add technical metadata
            record = manager.add_technical_metadata(
                record,
                volume_shape=[1024, 1024, 512],
                voxel_size_nm=[4.0, 4.0, 40.0],
                data_type="uint8",
                file_size_bytes=2147483648
            )
            
            # Validate against real schema
            validation = manager.validate_metadata(record)
            assert validation["valid"] is True, f"Validation errors: {validation['errors']}"
        else:
            pytest.skip("Real schema file not found")