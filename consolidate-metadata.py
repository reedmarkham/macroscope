import os
import json
import re
from datetime import datetime


def normalize_epfl_metadata(metadata, local_dir):
    return {
        "source": "epfl",
        "source_id": metadata.get("source_id", "EPFL"),
        "description": metadata.get("description", ""),
        "volume_shape": metadata.get("volume_shape"),
        "voxel_size_nm": metadata.get("voxel_size_nm"),
        "modality": metadata.get("modality", "electron microscopy"),
        "download_url": metadata.get("download_url", ""),
        "local_paths": metadata.get("local_paths", {}),
        "additional_metadata": {k: v for k, v in metadata.items() if k not in [
            "source", "source_id", "description", "volume_shape", "voxel_size_nm", "modality", "download_url", "local_paths"
        ]}
    }


def normalize_empiar_metadata(metadata, local_dir):
    return {
        "source": "empiar",
        "source_id": metadata.get("source_id", "EMPIAR"),
        "description": metadata.get("description", ""),
        "volume_shape": metadata.get("volume_shape"),
        "voxel_size_nm": metadata.get("voxel_size_nm"),
        "modality": metadata.get("modality", "electron microscopy"),
        "download_url": metadata.get("download_url", ""),
        "local_paths": metadata.get("local_paths", {}),
        "additional_metadata": {k: v for k, v in metadata.items() if k not in [
            "source", "source_id", "description", "volume_shape", "voxel_size_nm", "modality", "download_url", "local_paths"
        ]}
    }


def normalize_idr_metadata(metadata, local_dir):
    return {
        "source": "idr",
        "source_id": metadata.get("source_id", "IDR"),
        "description": metadata.get("description", ""),
        "volume_shape": metadata.get("volume_shape"),
        "voxel_size_nm": metadata.get("voxel_size_nm"),
        "modality": metadata.get("modality", "light microscopy"),
        "download_url": metadata.get("download_url", ""),
        "local_paths": metadata.get("local_paths", {}),
        "additional_metadata": {k: v for k, v in metadata.items() if k not in [
            "source", "source_id", "description", "volume_shape", "voxel_size_nm", "modality", "download_url", "local_paths"
        ]}
    }


def normalize_neuprint_metadata(metadata, local_dir):
    return {
        "source": "neuprint",
        "source_id": metadata.get("source_id", "neuprint"),
        "description": metadata.get("description", ""),
        "volume_shape": metadata.get("volume_shape"),
        "voxel_size_nm": metadata.get("voxel_size_nm"),
        "modality": metadata.get("modality", "connectomics"),
        "download_url": metadata.get("download_url", ""),
        "local_paths": metadata.get("local_paths", {}),
        "additional_metadata": {k: v for k, v in metadata.items() if k not in [
            "source", "source_id", "description", "volume_shape", "voxel_size_nm", "modality", "download_url", "local_paths"
        ]}
    }


def normalize_openorganelle_metadata(metadata, local_dir):
    return {
        "source": "openorganelle",
        "source_id": metadata.get("source_id", "openorganelle"),
        "description": metadata.get("description", ""),
        "volume_shape": metadata.get("volume_shape"),
        "voxel_size_nm": metadata.get("voxel_size_nm"),
        "modality": metadata.get("modality", "electron microscopy"),
        "download_url": metadata.get("download_url", ""),
        "local_paths": metadata.get("local_paths", {}),
        "additional_metadata": {k: v for k, v in metadata.items() if k not in [
            "source", "source_id", "description", "volume_shape", "voxel_size_nm", "modality", "download_url", "local_paths"
        ]}
    }


def extract_timestamp_from_filename(filename):
    # Matches metadata_20240528_153000.json or metadata_12345_20240528_153000.json
    match = re.search(r"metadata(?:_[^_]*)?_?(\d{8}_\d{6})\.json$", filename)
    if match:
        return match.group(1)
    return None

def consolidate_metadata(root_dir=".", output_file="metadata_catalog.json"):
    # Map: source -> (timestamp, filepath)
    latest_metadata = {}

    for subdir, _, files in os.walk(root_dir):
        for fname in files:
            if fname.startswith("metadata_") and fname.endswith(".json"):
                timestamp = extract_timestamp_from_filename(fname)
                if not timestamp:
                    continue
                path = os.path.join(subdir, fname)
                try:
                    with open(path) as f:
                        raw = json.load(f)
                    source = raw.get("source")
                    if not source:
                        continue
                    # If this source is not seen or this file is newer, update
                    if (source not in latest_metadata) or (timestamp > latest_metadata[source][0]):
                        latest_metadata[source] = (timestamp, path)
                except Exception as e:
                    print(f"Error reading {path}: {e}")

    consolidated = []
    for source, (timestamp, path) in latest_metadata.items():
        
        with open(path) as f:
            raw = json.load(f)
        if source == "epfl":
            consolidated.append(normalize_epfl_metadata(raw, os.path.dirname(path)))
        elif source == "empiar":
            consolidated.append(normalize_empiar_metadata(raw, os.path.dirname(path)))
        elif source == "idr":
            consolidated.append(normalize_idr_metadata(raw, os.path.dirname(path)))
        elif source == "neuprint":
            consolidated.append(normalize_neuprint_metadata(raw, os.path.dirname(path)))
        elif source == "openorganelle":
            consolidated.append(normalize_openorganelle_metadata(raw, os.path.dirname(path)))
        else:
            print(f"Skipping unrecognized metadata source in {path}")

    catalog_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    catalog_file = f"metadata_catalog_{catalog_timestamp}.json"
    
    with open(catalog_file, "w") as f:
        json.dump(consolidated, f, indent=2)
    print(f"Wrote consolidated metadata to {catalog_file}")


if __name__ == "__main__":
    consolidate_metadata()
