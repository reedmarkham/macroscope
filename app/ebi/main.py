import os
import json
from ftplib import FTP
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
import hashlib

import requests
import numpy as np
from tqdm import tqdm
from ncempy.io import ser
from dm3_lib import _dm3_lib as dm3

OUTPUT_DIR = 'empiar_volumes'
os.makedirs(OUTPUT_DIR, exist_ok=True)


def fetch_metadata(entry_id):
    api_url = f'https://www.ebi.ac.uk/empiar/api/entry/{entry_id}/'
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()


def is_file(ftp, filename):
    try:
        ftp.size(filename)
        return True
    except:
        return False


def download_files(entry_id, download_dir):
    ftp = FTP('ftp.ebi.ac.uk')
    ftp.login()
    ftp.cwd(f'/empiar/world_availability/{entry_id}/data/')
    filenames = ftp.nlst()

    downloaded_files = []
    os.makedirs(download_dir, exist_ok=True)

    for filename in tqdm(filenames, desc="Downloading files"):
        if not is_file(ftp, filename):
            print(f"⏭️ Skipping directory or invalid file: {filename}")
            continue
        local_path = os.path.join(download_dir, filename)
        with open(local_path, 'wb') as f:
            ftp.retrbinary(f'RETR {filename}', f.write)
        downloaded_files.append(local_path)

    ftp.quit()
    return downloaded_files


def load_volume(file_path):
    if file_path.endswith('.mrc'):
        import mrcfile
        with mrcfile.open(file_path, permissive=True) as mrc:
            return mrc.data.copy()
    elif file_path.endswith('.dm3'):
        dm3f = dm3.DM3(file_path)
        return dm3f.imagedata
    elif file_path.endswith('.ser'):
        data = ser.load_ser(file_path)
        return data['images'][0] if data['images'].shape[0] == 1 else data['images']
    else:
        raise ValueError(f"Unsupported file type: {file_path}")


def write_metadata_stub(entry_id, source_metadata, file_path, volume_path, metadata_path):
    stub = {
        "source": "empiar",
        "source_id": entry_id,
        "description": source_metadata.get("title", ""),
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "download_url": f"ftp://ftp.ebi.ac.uk/empiar/world_availability/{entry_id}/data/{os.path.basename(file_path)}",
        "local_paths": {
            "volume": volume_path,
            "raw": file_path,
            "metadata": metadata_path
        },
        "status": "saving-data",
        "additional_metadata": source_metadata,
    }
    tmp_path = metadata_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(stub, f, indent=2)
    os.replace(tmp_path, metadata_path)
    return stub


def enrich_metadata(metadata_path, stub, volume):
    stub.update({
        "volume_shape": list(volume.shape),
        "file_size_bytes": volume.nbytes,
        "sha256": hashlib.sha256(volume.tobytes()).hexdigest(),
        "status": "complete"
    })
    tmp_path = metadata_path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(stub, f, indent=2)
    os.replace(tmp_path, metadata_path)


def process_empiar_file(entry_id, source_metadata, file_path):
    try:
        volume = load_volume(file_path)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        base_name = os.path.splitext(os.path.basename(file_path))[0]
        volume_path = os.path.join(OUTPUT_DIR, f"{base_name}_{timestamp}.npy")
        metadata_path = volume_path.replace(".npy", "_metadata.json")

        # Write stub metadata first
        stub = write_metadata_stub(entry_id, source_metadata, file_path, volume_path, metadata_path)

        # Save volume
        np.save(volume_path, volume)

        # Enrich metadata
        enrich_metadata(metadata_path, stub, volume)

        return f"✅ Processed {file_path}"
    except Exception as e:
        return f"❌ Failed {file_path}: {e}"


def ingest_empiar(entry_id):
    metadata = fetch_metadata(entry_id)
    print(f"📥 Retrieved metadata for {entry_id}")
    download_dir = os.path.join(OUTPUT_DIR, 'downloads')
    os.makedirs(download_dir, exist_ok=True)

    file_paths = download_files(entry_id, download_dir)
    print(f"📦 Downloaded {len(file_paths)} files")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(process_empiar_file, entry_id, metadata, path)
            for path in file_paths
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing volumes"):
            print(future.result())


if __name__ == "__main__":
    ingest_empiar('11759')  # Example entry ID
