import os
import json

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

def consolidate_metadata(root_dir=".", output_file="metadata_catalog.json"):
    consolidated = []
    for subdir, _, files in os.walk(root_dir):
        if "metadata.json" in files:
            path = os.path.join(subdir, "metadata.json")
            with open(path) as f:
                raw = json.load(f)
            if raw.get("source") == "epfl":
                consolidated.append(normalize_epfl_metadata(raw, subdir))
            elif raw.get("source") == "empiar":
                consolidated.append(normalize_empiar_metadata(raw, subdir))
            else:
                print(f"Skipping unrecognized metadata source in {path}")
    with open(output_file, "w") as f:
        json.dump(consolidated, f, indent=2)
    print(f"Wrote consolidated metadata to {output_file}")


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

def consolidate_metadata(root_dir=".", output_file="metadata_catalog.json"):
    consolidated = []
    for subdir, _, files in os.walk(root_dir):
        if "metadata.json" in files:
            path = os.path.join(subdir, "metadata.json")
            with open(path) as f:
                raw = json.load(f)
            source = raw.get("source")
            if source == "epfl":
                consolidated.append(normalize_epfl_metadata(raw, subdir))
            elif source == "empiar":
                consolidated.append(normalize_empiar_metadata(raw, subdir))
            elif source == "idr":
                consolidated.append(normalize_idr_metadata(raw, subdir))
            elif source == "neuprint":
                consolidated.append(normalize_neuprint_metadata(raw, subdir))
            elif source == "openorganelle":
                consolidated.append(normalize_openorganelle_metadata(raw, subdir))
            else:
                print(f"Skipping unrecognized metadata source in {path}")
    with open(output_file, "w") as f:
        json.dump(consolidated, f, indent=2)
    print(f"Wrote consolidated metadata to {output_file}")
