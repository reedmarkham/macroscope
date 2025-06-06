"""
Metadata Management Library

Provides standardized metadata handling, validation, and status tracking
for the electron microscopy data ingestion pipeline.
"""

import json
import uuid
import os
import logging
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import jsonschema
from enum import Enum


class ProcessingStatus(Enum):
    """Standardized processing status values."""
    PENDING = "pending"
    PROCESSING = "processing"
    SAVING_DATA = "saving-data"
    COMPLETE = "complete"
    FAILED = "failed"
    CANCELLED = "cancelled"


class MetadataManager:
    """
    Centralized metadata management with schema validation and status tracking.
    """
    
    def __init__(self, schema_path: Optional[str] = None):
        """
        Initialize metadata manager.
        
        Args:
            schema_path: Path to JSON schema file. If None, uses default schema.
        """
        self.logger = logging.getLogger(__name__)
        
        if schema_path is None:
            # Default to schema in schemas directory relative to this file
            current_dir = Path(__file__).parent
            schema_path = current_dir.parent / "schemas" / "metadata_schema.json"
        
        self.schema_path = Path(schema_path)
        self._load_schema()
    
    def _load_schema(self) -> None:
        """Load and validate the JSON schema."""
        try:
            with open(self.schema_path, 'r') as f:
                self.schema = json.load(f)
            # Validate the schema itself
            jsonschema.Draft7Validator.check_schema(self.schema)
            self.logger.info(f"Loaded metadata schema from {self.schema_path}")
        except Exception as e:
            self.logger.error(f"Failed to load schema from {self.schema_path}: {e}")
            raise
    
    def create_metadata_record(
        self,
        source: str,
        source_id: str,
        description: str,
        initial_metadata: Optional[Dict[str, Any]] = None,
        record_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new standardized metadata record.
        
        Args:
            source: Data source identifier (ebi, epfl, flyem, idr, openorganelle)
            source_id: Unique identifier within the source
            description: Human-readable description
            initial_metadata: Additional metadata to include
            record_id: Specific ID to use (if None, generates UUID)
        
        Returns:
            Standardized metadata record
        """
        now = datetime.now(timezone.utc).isoformat()
        
        if record_id is None:
            record_id = str(uuid.uuid4())
        
        record = {
            "id": record_id,
            "source": source,
            "source_id": source_id,
            "status": ProcessingStatus.PENDING.value,
            "created_at": now,
            "updated_at": now,
            "metadata": {
                "core": {
                    "description": description
                }
            }
        }
        
        # Merge additional metadata if provided
        if initial_metadata:
            self._merge_metadata(record, initial_metadata)
        
        return record
    
    def update_status(
        self,
        record: Dict[str, Any],
        status: Union[ProcessingStatus, str],
        progress_percentage: Optional[float] = None,
        current_step: Optional[str] = None,
        error_message: Optional[str] = None,
        error_traceback: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Update the status and progress of a metadata record.
        
        Args:
            record: Metadata record to update
            status: New processing status
            progress_percentage: Progress percentage (0-100)
            current_step: Description of current processing step
            error_message: Error message if status is failed
            error_traceback: Full error traceback for debugging
        
        Returns:
            Updated metadata record
        """
        if isinstance(status, ProcessingStatus):
            status = status.value
        
        record["status"] = status
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        
        # Update progress information
        if progress_percentage is not None or current_step is not None:
            if "progress" not in record:
                record["progress"] = {}
            
            if progress_percentage is not None:
                record["progress"]["percentage"] = progress_percentage
            
            if current_step is not None:
                record["progress"]["current_step"] = current_step
        
        # Update error information
        if status == ProcessingStatus.FAILED.value:
            if "error" not in record:
                record["error"] = {"retry_count": 0}
            
            if error_message:
                record["error"]["message"] = error_message
            
            if error_traceback:
                record["error"]["traceback"] = error_traceback
            
            # Increment retry count
            record["error"]["retry_count"] = record["error"].get("retry_count", 0) + 1
        
        return record
    
    def add_technical_metadata(
        self,
        record: Dict[str, Any],
        volume_shape: Optional[List[int]] = None,
        voxel_size_nm: Optional[Union[List[float], Dict[str, float]]] = None,
        data_type: Optional[str] = None,
        file_size_bytes: Optional[int] = None,
        sha256: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Add technical metadata to a record.
        
        Args:
            record: Metadata record to update
            volume_shape: Shape of the volume data
            voxel_size_nm: Physical resolution per axis
            data_type: Data type of the volume array
            file_size_bytes: Size of the data file
            sha256: SHA256 hash of the data file
            **kwargs: Additional technical metadata
        
        Returns:
            Updated metadata record
        """
        # Initialize metadata structure if needed
        if "metadata" not in record:
            record["metadata"] = {}
        if "core" not in record["metadata"]:
            record["metadata"]["core"] = {}
        if "technical" not in record["metadata"]:
            record["metadata"]["technical"] = {}
        
        # Add core metadata
        if volume_shape is not None:
            record["metadata"]["core"]["volume_shape"] = volume_shape
        if voxel_size_nm is not None:
            record["metadata"]["core"]["voxel_size_nm"] = voxel_size_nm
        if data_type is not None:
            record["metadata"]["core"]["data_type"] = data_type
        
        # Add technical metadata
        if file_size_bytes is not None:
            record["metadata"]["technical"]["file_size_bytes"] = file_size_bytes
        if sha256 is not None:
            record["metadata"]["technical"]["sha256"] = sha256
        
        # Add any additional technical metadata
        for key, value in kwargs.items():
            record["metadata"]["technical"][key] = value
        
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return record
    
    def add_file_paths(
        self,
        record: Dict[str, Any],
        volume_path: Optional[str] = None,
        raw_path: Optional[str] = None,
        metadata_path: Optional[str] = None,
        **additional_paths
    ) -> Dict[str, Any]:
        """
        Add file path information to a record.
        
        Args:
            record: Metadata record to update
            volume_path: Path to processed volume file
            raw_path: Path to original raw data file
            metadata_path: Path to metadata file
            **additional_paths: Additional file paths
        
        Returns:
            Updated metadata record
        """
        if "files" not in record:
            record["files"] = {}
        
        if volume_path is not None:
            record["files"]["volume"] = volume_path
        if raw_path is not None:
            record["files"]["raw"] = raw_path
        if metadata_path is not None:
            record["files"]["metadata"] = metadata_path
        
        if additional_paths:
            if "additional" not in record["files"]:
                record["files"]["additional"] = {}
            record["files"]["additional"].update(additional_paths)
        
        record["updated_at"] = datetime.now(timezone.utc).isoformat()
        return record
    
    def validate_metadata(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Validate metadata record against schema.
        
        Args:
            record: Metadata record to validate
        
        Returns:
            Validation result with 'valid' boolean and 'errors' list
        """
        try:
            jsonschema.validate(record, self.schema)
            return {"valid": True, "errors": []}
        except jsonschema.ValidationError as e:
            return {"valid": False, "errors": [str(e)]}
        except Exception as e:
            return {"valid": False, "errors": [f"Validation failed: {str(e)}"]}
    
    def save_metadata(
        self,
        record: Dict[str, Any],
        output_path: str,
        validate: bool = True
    ) -> None:
        """
        Save metadata record to file with optional validation.
        
        Args:
            record: Metadata record to save
            output_path: Path where to save the metadata file
            validate: Whether to validate before saving
        
        Raises:
            ValueError: If validation fails and validate=True
        """
        if validate:
            validation_result = self.validate_metadata(record)
            if not validation_result["valid"]:
                raise ValueError(f"Metadata validation failed: {validation_result['errors']}")
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Atomic write using temporary file
        temp_path = f"{output_path}.tmp"
        try:
            with open(temp_path, 'w') as f:
                json.dump(record, f, indent=2, sort_keys=True)
            os.replace(temp_path, output_path)
            self.logger.info(f"Saved metadata to {output_path}")
        except Exception as e:
            # Clean up temporary file if it exists
            if os.path.exists(temp_path):
                os.remove(temp_path)
            raise e
    
    def load_metadata(self, file_path: str, validate: bool = True) -> Dict[str, Any]:
        """
        Load and optionally validate metadata from file.
        
        Args:
            file_path: Path to metadata file
            validate: Whether to validate after loading
        
        Returns:
            Loaded metadata record
        
        Raises:
            ValueError: If validation fails and validate=True
        """
        with open(file_path, 'r') as f:
            record = json.load(f)
        
        if validate:
            validation_result = self.validate_metadata(record)
            if not validation_result["valid"]:
                self.logger.warning(f"Loaded metadata failed validation: {validation_result['errors']}")
        
        return record
    
    def _merge_metadata(self, record: Dict[str, Any], additional: Dict[str, Any]) -> None:
        """
        Recursively merge additional metadata into record.
        
        Args:
            record: Base metadata record
            additional: Additional metadata to merge
        """
        for key, value in additional.items():
            if key in record and isinstance(record[key], dict) and isinstance(value, dict):
                self._merge_metadata(record[key], value)
            else:
                record[key] = value
    
    def get_processing_summary(self, metadata_files: List[str]) -> Dict[str, Any]:
        """
        Generate processing summary from multiple metadata files.
        
        Args:
            metadata_files: List of metadata file paths
        
        Returns:
            Summary statistics and status breakdown
        """
        summary = {
            "total_files": len(metadata_files),
            "status_counts": {},
            "source_counts": {},
            "validation_errors": [],
            "processing_times": []
        }
        
        for file_path in metadata_files:
            try:
                record = self.load_metadata(file_path, validate=False)
                
                # Count by status
                status = record.get("status", "unknown")
                summary["status_counts"][status] = summary["status_counts"].get(status, 0) + 1
                
                # Count by source
                source = record.get("source", "unknown")
                summary["source_counts"][source] = summary["source_counts"].get(source, 0) + 1
                
                # Validate and collect errors
                validation = self.validate_metadata(record)
                if not validation["valid"]:
                    summary["validation_errors"].append({
                        "file": file_path,
                        "errors": validation["errors"]
                    })
                
                # Calculate processing time if available
                if "created_at" in record and "updated_at" in record:
                    try:
                        created = datetime.fromisoformat(record["created_at"].replace('Z', '+00:00'))
                        updated = datetime.fromisoformat(record["updated_at"].replace('Z', '+00:00'))
                        processing_time = (updated - created).total_seconds()
                        summary["processing_times"].append(processing_time)
                    except:
                        pass  # Skip invalid timestamps
                        
            except Exception as e:
                summary["validation_errors"].append({
                    "file": file_path,
                    "errors": [f"Failed to load file: {str(e)}"]
                })
        
        return summary