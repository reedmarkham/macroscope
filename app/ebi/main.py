import os
import json
from ftplib import FTP
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests
import numpy as np
from tqdm import tqdm

OUTPUT_DIR = 'empiar_volumes'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def fetch_metadata(entry_id):
    api_url = f'https://www.ebi.ac.uk/empiar/api/entry/{entry_id}/'
    response = requests.get(api_url)
    response.raise_for_status()
    return response.json()

def download_files(entry_id, download_dir):
    ftp = FTP('ftp.ebi.ac.uk')
    ftp.login()
    ftp.cwd(f'/empiar/world_availability/{entry_id}/data/')
    filenames = ftp.nlst()
    downloaded_files = []
    for filename in tqdm(filenames, desc="Downloading files"):
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
        from pyDM3reader import DM3Reader
        with open(file_path, 'rb') as f:
            dm3 = DM3Reader(f)
            return np.array(dm3.imagedata)
    elif file_path.endswith('.ser'):
        from ncempy.io import ser
        data = ser.load_ser(file_path)
        return data['images'][0] if data['images'].shape[0] == 1 else data['images']
    else:
        raise ValueError(f"Unsupported file type: {file_path}")

def write_metadata(entry_id, source_metadata, file_path, volume_shape, volume_path):
    metadata = {
        "source": "empiar",
        "source_id": entry_id,
        "description": source_metadata.get("title", ""),
        "modality": "electron microscopy",
        "volume_shape": list(volume_shape),
        "voxel_size_nm": None,
        "download_url": f"ftp://ftp.ebi.ac.uk/empiar/world_availability/{entry_id}/data/{os.path.basename(file_path)}",
        "local_paths": {
            "volume": volume_path,
            "raw": file_path,
            "metadata": volume_path.replace(".npy", "_metadata.json")
        },
        "additional_metadata": source_metadata
    }
    with open(metadata["local_paths"]["metadata"], "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to {metadata['local_paths']['metadata']}")

def process_empiar_file(entry_id, source_metadata, file_path):
    try:
        volume = load_volume(file_path)
        volume_path = os.path.join(OUTPUT_DIR, os.path.splitext(os.path.basename(file_path))[0] + ".npy")
        np.save(volume_path, volume)
        write_metadata(entry_id, source_metadata, file_path, volume.shape, volume_path)
        return f"Processed {file_path}"
    except Exception as e:
        return f"Failed {file_path}: {e}"

def ingest_empiar(entry_id):
    metadata = fetch_metadata(entry_id)
    print(f"Retrieved metadata for {entry_id}")
    download_dir = os.path.join(OUTPUT_DIR, 'downloads')
    os.makedirs(download_dir, exist_ok=True)

    file_paths = download_files(entry_id, download_dir)
    print(f"Downloaded {len(file_paths)} files")

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(process_empiar_file, entry_id, metadata, path)
            for path in file_paths
        ]
        for future in tqdm(as_completed(futures), total=len(futures), desc="Processing volumes"):
            print(future.result())

if __name__ == "__main__":
    ingest_empiar('EMPIAR-11759')  # Example entry ID