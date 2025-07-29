#!/usr/bin/env python3
"""
Quick validation script to check the latest metadata files
"""

import sys
import os
sys.path.append('/app/lib')

from metadata_manager import MetadataManager
import json
from pathlib import Path

def validate_latest_files():
    """Validate the most recent metadata files from each source"""
    metadata_manager = MetadataManager()
    
    # Data directories to check
    data_dirs = [
        '/app/data/ebi',
        '/app/data/epfl', 
        '/app/data/flyem',
        '/app/data/idr',
        '/app/data/openorganelle'
    ]
    
    print("Validating latest metadata files...")
    print("=" * 50)
    
    total_files = 0
    valid_files = 0
    
    for data_dir in data_dirs:
        if not os.path.exists(data_dir):
            continue
            
        source_name = os.path.basename(data_dir)
        print(f"\n{source_name.upper()}:")
        
        # Find metadata files
        metadata_files = list(Path(data_dir).glob("*metadata*.json"))
        if not metadata_files:
            print("  No metadata files found")
            continue
            
        # Get the most recent file
        latest_file = max(metadata_files, key=lambda x: x.stat().st_mtime)
        
        try:
            with open(latest_file, 'r') as f:
                metadata = json.load(f)
            
            validation_result = metadata_manager.validate_metadata(metadata)
            total_files += 1
            
            if validation_result["valid"]:
                valid_files += 1
                print(f"  VALID: {latest_file.name}")
            else:
                print(f"  INVALID: {latest_file.name}")
                for error in validation_result["errors"][:2]:  # Show first 2 errors
                    print(f"     Error: {error}")
                    
        except Exception as e:
            total_files += 1
            print(f"  ERROR: {latest_file.name} - {e}")
    
    print("\n" + "=" * 50)
    print(f"SUMMARY: {valid_files}/{total_files} files valid ({valid_files/total_files*100:.1f}%)")
    
    return valid_files == total_files

if __name__ == "__main__":
    success = validate_latest_files()
    sys.exit(0 if success else 1)