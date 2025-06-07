#!/usr/bin/env python3
"""
Clean up old metadata files that don't comply with v2.0 schema
This will help get accurate validation reports
"""

import sys
import os
sys.path.append('/app/lib')

from metadata_manager import MetadataManager
import json
from pathlib import Path
from datetime import datetime

def cleanup_old_metadata():
    """Remove metadata files that fail v2.0 schema validation"""
    metadata_manager = MetadataManager()
    
    # Data directories to check
    data_dirs = [
        '/app/data/ebi',
        '/app/data/epfl', 
        '/app/data/flyem',
        '/app/data/idr',
        '/app/data/openorganelle'
    ]
    
    print("Cleaning up old non-compliant metadata files...")
    print("=" * 60)
    
    total_files = 0
    removed_files = 0
    
    for data_dir in data_dirs:
        if not os.path.exists(data_dir):
            continue
            
        source_name = os.path.basename(data_dir)
        print(f"\n{source_name.upper()}:")
        
        # Find all metadata files
        metadata_files = list(Path(data_dir).glob("*metadata*.json"))
        if not metadata_files:
            print("  No metadata files found")
            continue
        
        source_removed = 0
        for metadata_file in metadata_files:
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                
                validation_result = metadata_manager.validate_metadata(metadata)
                total_files += 1
                
                if not validation_result["valid"]:
                    # Check if it's missing the 'id' field (primary indicator of old format)
                    if any("'id' is a required property" in error for error in validation_result["errors"]):
                        print(f"  🗑️  Removing {metadata_file.name} (old format)")
                        metadata_file.unlink()
                        removed_files += 1
                        source_removed += 1
                    else:
                        print(f"  ⚠️  Keeping {metadata_file.name} (validation error but has 'id' field)")
                else:
                    print(f"  ✅ Keeping {metadata_file.name} (valid)")
                    
            except Exception as e:
                print(f"  💥 Error reading {metadata_file.name}: {e}")
                total_files += 1
        
        if source_removed > 0:
            print(f"  Removed {source_removed} old metadata files from {source_name}")
    
    print("\n" + "=" * 60)
    print(f"CLEANUP SUMMARY:")
    print(f"  Total files examined: {total_files}")
    print(f"  Old files removed: {removed_files}")
    print(f"  Files remaining: {total_files - removed_files}")
    
    if removed_files > 0:
        print(f"\n✅ Cleaned up {removed_files} old metadata files")
        print("Re-run the pipeline to generate fresh v2.0 compliant metadata")
    else:
        print("\n✅ No old metadata files found - all files are v2.0 compliant!")

if __name__ == "__main__":
    cleanup_old_metadata()