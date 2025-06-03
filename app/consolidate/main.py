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

def consolidate_metadata_key_ratios(root_dir: str = "..") -> None:
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
        # Skip the 'consolidate' folder itself
        if os.path.basename(subdir) == "consolidate":
            continue

        for fname in files:
            if "metadata" in fname and fname.endswith(".json"):
                metadata_files.append(os.path.join(subdir, fname))

    print(f"📂 Found {len(metadata_files)} candidate metadata files.\n")

    # Track total files per source
    source_file_counts: Dict[str, int] = defaultdict(int)
    # key_source_counts[key][source] = count of files (with that key in metadata)
    key_source_counts: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    distinct_keys: set = set()

    for path in metadata_files:
        rel_path = os.path.relpath(path, root_dir)
        # Determine the source as the top-level subdirectory under root_dir
        parts = rel_path.split(os.sep)
        source_dir = parts[0] if len(parts) > 1 else "."

        source_file_counts[source_dir] += 1

        try:
            with open(path) as f:
                metadata: Dict[str, Any] = json.load(f)

            keys: List[str] = list(metadata.keys())
            short_name: str = os.path.basename(path)
            print(f"📄 Processed {short_name} (source={source_dir}, {len(keys)} keys)")

            for key in keys:
                distinct_keys.add(key)
                key_source_counts[key][source_dir] += 1

        except Exception as e:
            print(f"❌ Failed to read {path}: {e}")

    # Build ratio structure: key -> { source: ratio }
    key_ratios_by_source: Dict[str, Dict[str, float]] = {}
    for key, source_counts in key_source_counts.items():
        key_ratios_by_source[key] = {}
        for source, count in source_counts.items():
            total = source_file_counts.get(source, 1)
            ratio = count / total
            key_ratios_by_source[key][source] = round(ratio, 4)  # rounded to 4 decimal places

    # Prepare catalog structure
    catalog = {
        "distinct_keys": sorted(distinct_keys),
        "key_ratios_by_source": key_ratios_by_source,
        "total_files_by_source": source_file_counts
    }

    # Write JSON summary
    with open(catalog_file, "w") as f:
        json.dump(catalog, f, indent=2)

    print(f"\n🔑 Found {len(distinct_keys)} distinct keys.")
    print(f"✅ Wrote metadata key ratio summary to {catalog_file}")
    print(f"📝 Log written to {log_file}")

if __name__ == "__main__":
    consolidate_metadata_key_ratios()
