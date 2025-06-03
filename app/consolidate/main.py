import os
import json
import re
from datetime import datetime
from collections import defaultdict
import sys
from typing import Optional, List, Dict, Any

def extract_timestamp_from_filename(filename: str) -> Optional[str]:
    if "metadata" not in filename:
        return None
    match = re.search(r"(\d{8}_\d{6})", filename)
    return match.group(1) if match else None

def consolidate_metadata_keys_only(root_dir: str = "..") -> None:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"metadata_catalog_{timestamp}.log"
    catalog_file = f"metadata_catalog_{timestamp}.json"

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

    metadata_files: List[str] = []
    for subdir, _, files in os.walk(root_dir):
        for fname in files:
            if "metadata" in fname and fname.endswith(".json"):
                metadata_files.append(os.path.join(subdir, fname))

    print(f"📂 Found {len(metadata_files)} candidate metadata files.\n")

    grouped_keys: Dict[str, Dict[str, List[str]]] = defaultdict(dict)

    for path in metadata_files:
        try:
            with open(path) as f:
                metadata: Dict[str, Any] = json.load(f)

            status: str = metadata.get("status", "unknown")
            keys: List[str] = list(metadata.keys())
            short_name: str = os.path.basename(path)

            grouped_keys[status][short_name] = keys
            print(f"📄 Processed {short_name} (status={status}, {len(keys)} keys)")

        except Exception as e:
            print(f"❌ Failed to read {path}: {e}")

    with open(catalog_file, "w") as f:
        json.dump(grouped_keys, f, indent=2)

    print(f"\n✅ Wrote metadata key summary to {catalog_file}")
    print(f"📝 Log written to {log_file}")

if __name__ == "__main__":
    consolidate_metadata_keys_only()
