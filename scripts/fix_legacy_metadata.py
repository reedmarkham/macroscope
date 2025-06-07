#!/usr/bin/env python3
"""
Fix legacy metadata files to comply with v2.0 schema
This script handles:
1. Converting 'empiar' source to 'ebi' 
2. Converting old flat structure to new nested schema structure
3. Adding required fields (id, created_at, status, metadata object)
"""

import sys
import os
import json
import uuid
from pathlib import Path
from datetime import datetime, timezone

# Add lib directory to path for metadata_manager import
sys.path.append('/app/lib')
from metadata_manager import MetadataManager

def fix_empiar_source_files():
    """Fix files that have source='empiar' to source='ebi'"""
    ebi_dir = Path('/app/data/ebi')
    fixed_count = 0
    
    print("Fixing EMPIAR source files...")
    
    for metadata_file in ebi_dir.glob("*metadata*.json"):
        try:
            with open(metadata_file, 'r') as f:
                data = json.load(f)
            
            if data.get('source') == 'empiar':
                print(f"  Fixing {metadata_file.name}: empiar -> ebi")
                data['source'] = 'ebi'
                
                # Write back with proper formatting
                with open(metadata_file, 'w') as f:
                    json.dump(data, f, indent=2)
                fixed_count += 1
                
        except Exception as e:
            print(f"  Error fixing {metadata_file.name}: {e}")
    
    print(f"Fixed {fixed_count} EMPIAR source files")
    return fixed_count

def convert_old_metadata_to_schema(metadata_file: Path) -> bool:
    """Convert old flat metadata structure to new v2.0 schema structure"""
    try:
        with open(metadata_file, 'r') as f:
            old_data = json.load(f)
        
        # Check if already in new format (has 'id' and 'metadata' fields)
        if 'id' in old_data and 'metadata' in old_data:
            return False  # Already in new format
        
        # Initialize MetadataManager for schema compliance
        metadata_manager = MetadataManager()
        
        # Extract required fields from old format
        source = old_data.get('source', 'unknown')
        source_id = old_data.get('source_id', 'unknown')
        description = old_data.get('description', f"Data from {source}")
        
        # Create new schema-compliant record
        new_record = metadata_manager.create_metadata_record(
            source=source,
            source_id=source_id,
            description=description
        )
        
        # Map old fields to new structure
        core_metadata = new_record["metadata"]["core"]
        technical_metadata = new_record["metadata"]["technical"]
        
        # Map common fields to technical metadata
        if 'volume_shape' in old_data:
            technical_metadata['volume_shape'] = old_data['volume_shape']
        if 'dtype' in old_data:
            core_metadata['data_type'] = old_data['dtype']
        if 'file_size_bytes' in old_data:
            technical_metadata['file_size_bytes'] = old_data['file_size_bytes']
        if 'sha256' in old_data:
            technical_metadata['sha256'] = old_data['sha256']
        if 'global_mean' in old_data:
            technical_metadata['global_mean'] = old_data['global_mean']
        if 'chunk_size' in old_data:
            technical_metadata['chunk_size'] = old_data['chunk_size']
        if 'processing_time_seconds' in old_data:
            technical_metadata['processing_time_seconds'] = old_data['processing_time_seconds']
        if 'chunk_strategy' in old_data:
            technical_metadata['chunk_strategy'] = old_data['chunk_strategy']
        
        # Map voxel size information
        if 'voxel_size_nm' in old_data:
            core_metadata['voxel_size_nm'] = old_data['voxel_size_nm']
        
        # Map dimensions for OpenOrganelle files
        if 'dimensions_nm' in old_data:
            dims = old_data['dimensions_nm']
            if isinstance(dims, dict):
                # Convert dict to array format for schema compliance
                technical_metadata['dimensions_nm'] = [
                    dims.get('x', 0),
                    dims.get('y', 0),
                    dims.get('z', 0)
                ]
            else:
                technical_metadata['dimensions_nm'] = dims
        
        # Add provenance information
        provenance = new_record["metadata"].setdefault("provenance", {})
        if 'download_url' in old_data:
            provenance['download_url'] = old_data['download_url']
        if 'internal_zarr_path' in old_data:
            provenance['internal_zarr_path'] = old_data['internal_zarr_path']
        
        # Map file paths
        if 'local_paths' in old_data:
            new_record['files'] = {
                'volume': old_data['local_paths'].get('volume', ''),
                'metadata': old_data['local_paths'].get('metadata', str(metadata_file))
            }
        
        # Preserve additional metadata
        if 'additional_metadata' in old_data:
            new_record['additional_metadata'] = old_data['additional_metadata']
        
        # Set status and timestamps
        new_record['status'] = old_data.get('status', 'complete')
        
        # Use existing timestamp if available, otherwise use file modification time
        if 'timestamp' in old_data:
            try:
                # Try to parse existing timestamp
                if old_data['timestamp'].endswith('Z'):
                    new_record['created_at'] = old_data['timestamp']
                else:
                    # Convert to ISO format if needed
                    dt = datetime.fromisoformat(old_data['timestamp'].replace('Z', '+00:00'))
                    new_record['created_at'] = dt.isoformat()
            except:
                # Fallback to file modification time
                mtime = metadata_file.stat().st_mtime
                dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
                new_record['created_at'] = dt.isoformat()
        else:
            # Use file modification time
            mtime = metadata_file.stat().st_mtime
            dt = datetime.fromtimestamp(mtime, tz=timezone.utc)
            new_record['created_at'] = dt.isoformat()
        
        # Save the converted metadata
        metadata_manager.save_metadata(new_record, str(metadata_file), validate=True)
        return True
        
    except Exception as e:
        print(f"  Error converting {metadata_file.name}: {e}")
        return False

def fix_all_metadata_files():
    """Convert all old metadata files to new schema format"""
    data_dirs = [
        Path('/app/data/ebi'),
        Path('/app/data/epfl'),
        Path('/app/data/flyem'),
        Path('/app/data/idr'),
        Path('/app/data/openorganelle')
    ]
    
    total_converted = 0
    
    print("\nConverting old metadata files to v2.0 schema...")
    
    for data_dir in data_dirs:
        if not data_dir.exists():
            continue
            
        source_name = data_dir.name
        print(f"\n{source_name.upper()}:")
        
        metadata_files = list(data_dir.glob("*metadata*.json"))
        if not metadata_files:
            print("  No metadata files found")
            continue
        
        converted_count = 0
        for metadata_file in metadata_files:
            if convert_old_metadata_to_schema(metadata_file):
                print(f"  Converted {metadata_file.name}")
                converted_count += 1
            else:
                print(f"  Skipped {metadata_file.name} (already in new format)")
        
        if converted_count > 0:
            print(f"  Converted {converted_count} files in {source_name}")
        total_converted += converted_count
    
    print(f"\nTotal files converted: {total_converted}")
    return total_converted

def main():
    print("Legacy Metadata Fix Script")
    print("=" * 50)
    
    # Step 1: Fix EMPIAR source files
    empiar_fixed = fix_empiar_source_files()
    
    # Step 2: Convert old metadata files to new schema
    schema_converted = fix_all_metadata_files()
    
    print("\n" + "=" * 50)
    print("MIGRATION SUMMARY:")
    print(f"  EMPIAR source fixes: {empiar_fixed}")
    print(f"  Schema conversions: {schema_converted}")
    print(f"  Total fixes applied: {empiar_fixed + schema_converted}")
    
    if empiar_fixed + schema_converted > 0:
        print("\nFiles have been updated to v2.0 schema format.")
        print("Re-run validation to verify schema compliance.")
    else:
        print("\nNo legacy files found - all metadata is already v2.0 compliant!")

if __name__ == "__main__":
    main()