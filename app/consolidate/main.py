import os
import json
import re
import logging
from datetime import datetime
from collections import defaultdict
import sys
from pathlib import Path
from typing import Optional, List, Dict, Any

# Add lib directory to path for config_manager import
sys.path.append('/app/lib')

from metadata_manager import MetadataManager
from config_manager import get_config_manager

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

def extract_timestamp_from_filename(filename: str) -> Optional[str]:
    if "metadata" not in filename:
        return None
    match = re.search(r"(\d{8}_\d{6})", filename)
    return match.group(1) if match else None

def consolidate_metadata_with_validation(config_path: Optional[str] = None) -> None:
    """
    Enhanced metadata consolidation with schema validation and configuration management.
    
    Args:
        config_path: Path to configuration file. If None, uses default.
    """
    # Initialize configuration and metadata managers
    config_manager = get_config_manager(config_path)
    metadata_manager = MetadataManager()
    
    # Get consolidation configuration
    consolidation_config = config_manager.get_consolidation_config()
    
    # Setup output files
    timestamp = datetime.now().strftime(consolidation_config.get('processing', {}).get('timestamp_format', '%Y%m%d_%H%M%S'))
    output_dir = Path(consolidation_config.get('output_dir', './app/consolidate'))
    output_dir.mkdir(parents=True, exist_ok=True)
    
    log_file = output_dir / f"metadata_catalog_{timestamp}.log"
    catalog_file = output_dir / f"metadata_catalog_{timestamp}.json"
    validation_report_file = output_dir / f"validation_report_{timestamp}.json"

    # Tee stdout to both console and log file
    class Logger:
        def __init__(self, *streams):
            self.streams = streams

        def write(self, message: str) -> None:
            for s in self.streams:
                s.write(message)
                s.flush()

        def flush(self) -> None:
            for s in self.streams:
                s.flush()

    sys.stdout = Logger(sys.stdout, open(log_file, "w"))
    
    logger.info("Starting enhanced metadata consolidation with validation")
    logger.info("Schema validation: %s", 'enabled' if consolidation_config.get('validation', {}).get('strict_mode', False) else 'reporting only')
    logger.info("Scanning directories: %s", consolidation_config.get('processing', {}).get('scan_directories', []))

    # Get scan directories from configuration
    scan_dirs = consolidation_config.get('processing', {}).get('scan_directories', ['..'])
    include_patterns = consolidation_config.get('processing', {}).get('include_patterns', ['metadata*.json'])
    exclude_patterns = consolidation_config.get('processing', {}).get('exclude_patterns', ['*.tmp', '*.log', '*.backup'])
    
    metadata_files: List[str] = []
    
    for root_dir in scan_dirs:
        root_path = Path(root_dir)
        if not root_path.exists():
            logger.warning("Scan directory does not exist: %s", root_path)
            continue
            
        for subdir, _, files in os.walk(root_path):
            # Skip the 'consolidate' folder itself
            if os.path.basename(subdir) == "consolidate":
                continue

            for fname in files:
                # Check include patterns
                if not any(fname.startswith(pattern.replace('*', '')) or fname.endswith(pattern.replace('*', '')) 
                          for pattern in include_patterns):
                    continue
                
                # Check exclude patterns
                if any(fname.endswith(pattern.replace('*', '')) for pattern in exclude_patterns):
                    continue
                    
                metadata_files.append(os.path.join(subdir, fname))

    logger.info("Found %d candidate metadata files", len(metadata_files))

    # Enhanced tracking with validation
    source_file_counts: Dict[str, int] = defaultdict(int)
    key_source_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    distinct_keys: set = set()
    validation_results: List[Dict[str, Any]] = []
    valid_records: List[Dict[str, Any]] = []
    processing_summary = {
        'total_files': len(metadata_files),
        'processed_files': 0,
        'validation_errors': 0,
        'parsing_errors': 0,
        'sources_found': set()
    }

    for path in metadata_files:
        try:
            # Load and validate metadata
            metadata = metadata_manager.load_metadata(path, validate=False)
            processing_summary['processed_files'] += 1
            
            # Determine source from metadata or file path
            source = metadata.get('source')
            if not source:
                # Fallback to directory-based source detection
                for scan_dir in scan_dirs:
                    rel_path = os.path.relpath(path, scan_dir)
                    parts = rel_path.split(os.sep)
                    if len(parts) > 1:
                        potential_source = parts[0]
                        if potential_source in ['ebi', 'epfl', 'flyem', 'idr', 'openorganelle']:
                            source = potential_source
                            break
                
                if not source:
                    source = "unknown"
            
            processing_summary['sources_found'].add(source)
            source_file_counts[source] += 1
            
            # Validate against schema
            validation_result = metadata_manager.validate_metadata(metadata)
            validation_results.append({
                'file': path,
                'source': source,
                'valid': validation_result['valid'],
                'errors': validation_result['errors']
            })
            
            if not validation_result['valid']:
                processing_summary['validation_errors'] += 1
                logger.warning("Validation failed for %s (%s): %d errors", os.path.basename(path), source, len(validation_result['errors']))
            else:
                valid_records.append(metadata)
                logger.info("Valid metadata: %s (%s)", os.path.basename(path), source)
            
            # Track keys regardless of validation status (for backward compatibility)
            keys: List[str] = list(metadata.keys())
            for key in keys:
                distinct_keys.add(key)
                key_source_counts[key][source] += 1

        except json.JSONDecodeError as e:
            processing_summary['parsing_errors'] += 1
            logger.error("JSON parsing failed for %s: %s", path, e)
            validation_results.append({
                'file': path,
                'source': 'unknown',
                'valid': False,
                'errors': [f"JSON parsing error: {str(e)}"]
            })
        except Exception as e:
            processing_summary['parsing_errors'] += 1
            logger.error("Failed to process %s: %s", path, e)
            validation_results.append({
                'file': path,
                'source': 'unknown', 
                'valid': False,
                'errors': [f"Processing error: {str(e)}"]
            })

    # Convert sources_found set to list for JSON serialization
    processing_summary['sources_found'] = list(processing_summary['sources_found'])
    
    # Build ratio structure: key -> { source: ratio }
    key_ratios_by_source: Dict[str, Dict[str, float]] = {}
    for key, source_counts in key_source_counts.items():
        key_ratios_by_source[key] = {}
        for source, count in source_counts.items():
            total = source_file_counts.get(source, 1)
            ratio = count / total
            key_ratios_by_source[key][source] = round(ratio, 4)
    
    # Calculate validation statistics
    validation_stats = {
        'total_files': len(validation_results),
        'valid_files': sum(1 for r in validation_results if r['valid']),
        'invalid_files': sum(1 for r in validation_results if not r['valid']),
        'validation_rate': round(sum(1 for r in validation_results if r['valid']) / len(validation_results) * 100, 2) if validation_results else 0,
        'errors_by_source': {}
    }
    
    # Group validation errors by source
    for result in validation_results:
        source = result['source']
        if source not in validation_stats['errors_by_source']:
            validation_stats['errors_by_source'][source] = {'valid': 0, 'invalid': 0, 'error_details': []}
        
        if result['valid']:
            validation_stats['errors_by_source'][source]['valid'] += 1
        else:
            validation_stats['errors_by_source'][source]['invalid'] += 1
            validation_stats['errors_by_source'][source]['error_details'].extend(result['errors'])
    
    # Enhanced catalog structure
    catalog = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "schema_version": "1.0",
            "consolidation_version": "2.0-enhanced",
            "configuration": {
                "validation_enabled": consolidation_config.get('validation', {}).get('strict_mode', False),
                "scan_directories": scan_dirs,
                "include_patterns": include_patterns,
                "exclude_patterns": exclude_patterns
            }
        },
        "summary": processing_summary,
        "validation": validation_stats,
        "legacy_compatibility": {
            "distinct_keys": sorted(distinct_keys),
            "key_ratios_by_source": key_ratios_by_source,
            "total_files_by_source": dict(source_file_counts)
        },
        "valid_records_sample": valid_records[:5] if valid_records else [],  # Include first 5 valid records as examples
        "data_quality": {
            "completeness_by_source": {},
            "common_missing_fields": {},
            "schema_compliance_rate": validation_stats['validation_rate']
        }
    }
    
    # Calculate data quality metrics
    required_fields = consolidation_config.get('quality', {}).get('min_required_fields', ['source', 'source_id', 'description', 'status'])
    for source in processing_summary['sources_found']:
        source_records = [r for r in valid_records if r.get('source') == source]
        if source_records:
            total_fields = sum(len(r.keys()) for r in source_records) / len(source_records)
            required_present = sum(all(field in r for field in required_fields) for r in source_records) / len(source_records)
            catalog["data_quality"]["completeness_by_source"][source] = {
                "average_fields": round(total_fields, 2),
                "required_fields_present": round(required_present * 100, 2)
            }
    
    # Write enhanced catalog
    with open(catalog_file, "w") as f:
        json.dump(catalog, f, indent=2, sort_keys=True)
    
    # Write detailed validation report
    validation_report = {
        "generated_at": datetime.now().isoformat(),
        "summary": validation_stats,
        "detailed_results": validation_results
    }
    
    with open(validation_report_file, "w") as f:
        json.dump(validation_report, f, indent=2, sort_keys=True)
    
    # Log enhanced summary
    logger.info("CONSOLIDATION SUMMARY")
    logger.info("Found %d distinct metadata keys across %d sources", len(distinct_keys), len(processing_summary['sources_found']))
    logger.info("Processed %d/%d files", processing_summary['processed_files'], processing_summary['total_files'])
    logger.info("Schema validation: %d valid, %d invalid (%.1f%% valid)", validation_stats['valid_files'], validation_stats['invalid_files'], validation_stats['validation_rate'])
    logger.info("Sources found: %s", ', '.join(processing_summary['sources_found']))
    logger.info("Output files:")
    logger.info("  Enhanced catalog: %s", catalog_file)
    logger.info("  Validation report: %s", validation_report_file)
    logger.info("  Processing log: %s", log_file)
    
    if validation_stats['invalid_files'] > 0:
        logger.warning("%d files failed validation. See %s for details.", validation_stats['invalid_files'], validation_report_file)
    
    # Warn about configuration issues
    strict_mode = consolidation_config.get('validation', {}).get('strict_mode', False)
    if strict_mode and validation_stats['invalid_files'] > 0:
        logger.error("STRICT MODE: %d validation failures detected!", validation_stats['invalid_files'])
        if consolidation_config.get('validation', {}).get('fail_on_invalid', False):
            sys.exit(1)

def consolidate_metadata_key_ratios(root_dir: str = "..") -> None:
    """
    Legacy function for backward compatibility.
    Calls the enhanced consolidation with default settings.
    """
    logger.warning("Using legacy consolidation function. Consider upgrading to consolidate_metadata_with_validation()")
    
    # Create a minimal configuration for legacy mode
    legacy_config = {
        'processing': {
            'scan_directories': [root_dir],
            'include_patterns': ['metadata*.json'],
            'exclude_patterns': ['*.tmp', '*.log'],
            'timestamp_format': '%Y%m%d_%H%M%S'
        },
        'output_dir': './app/consolidate',
        'validation': {
            'strict_mode': False,
            'report_validation_errors': True,
            'fail_on_invalid': False
        }
    }
    
    # Temporarily replace configuration for legacy call
    from lib.config_manager import _config_manager
    original_config = None
    if _config_manager:
        original_config = _config_manager._config
        _config_manager._config = {'consolidation': legacy_config}
    
    try:
        consolidate_metadata_with_validation()
    finally:
        if original_config and _config_manager:
            _config_manager._config = original_config


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Enhanced metadata consolidation with validation')
    parser.add_argument('--config', type=str, help='Path to configuration file')
    parser.add_argument('--legacy', action='store_true', help='Use legacy consolidation mode')
    parser.add_argument('--root-dir', type=str, default='..', help='Root directory for legacy mode')
    
    args = parser.parse_args()
    
    if args.legacy:
        consolidate_metadata_key_ratios(args.root_dir)
    else:
        consolidate_metadata_with_validation(args.config)
