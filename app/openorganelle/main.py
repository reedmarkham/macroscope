import os
import json

import zarr
import s3fs
import numpy as np
import dask.array as da


def load_zarr_from_s3(bucket_uri: str) -> da.Array:
    if not bucket_uri.startswith("s3://"):
        raise ValueError("URI must start with s3://")

    bucket_uri = bucket_uri[5:]
    bucket, prefix = bucket_uri.split("/", 1)

    s3 = s3fs.S3FileSystem(anon=True)
    store = s3fs.S3Map(root=prefix, s3=s3, check=False, bucket=bucket)

    try:
        zarr_group = zarr.open_consolidated(store)
    except zarr.errors.MetaDataError:
        zarr_group = zarr.open_group(store)

    return da.from_zarr(zarr_group)


def summarize_data(data: da.Array) -> dict:
    shape = data.shape
    dtype = str(data.dtype)
    chunk_size = data.chunksize
    mean_val = data.mean().compute()
    print("Zarr shape:", shape)
    print("Zarr dtype:", dtype)
    print("Zarr chunk size:", chunk_size)
    print("Global mean value:", mean_val)

    return {
        "volume_shape": shape,
        "dtype": dtype,
        "chunk_size": chunk_size,
        "global_mean": float(mean_val)
    }


def save_full_volume(data: da.Array, output_dir: str, filename: str = "full_volume.npy") -> str:
    os.makedirs(output_dir, exist_ok=True)
    volume = data.compute()
    output_path = os.path.join(output_dir, filename)
    np.save(output_path, volume)
    print(f"Saved full Zarr volume to {output_path}")
    return output_path


def write_metadata(output_dir: str, s3_uri: str, data_summary: dict, local_path: str):
    metadata = {
        "source": "openorganelle",
        "source_id": os.path.basename(s3_uri).replace(".zarr", ""),
        "description": "OpenOrganelle dataset ingested from Zarr on S3",
        "modality": "electron microscopy",
        "download_url": s3_uri,
        "imaging_start_date": "Mon Mar 09 2015",
        "voxel_size_nm": {"x": 4.0, "y": 4.0, "z": 2.96},
        "dimensions_nm": {"x": 10384, "y": 10080, "z": 1669.44},
        "local_paths": {
            "volume": local_path,
            "metadata": os.path.join(output_dir, "metadata.json")
        },
        **data_summary
    }

    with open(metadata["local_paths"]["metadata"], "w") as f:
        json.dump(metadata, f, indent=2)
    print(f"Saved metadata to {metadata['local_paths']['metadata']}")


if __name__ == "__main__":
    s3_uri = "s3://janelia-cosem-datasets/jrc_mus-nacc-2/jrc_mus-nacc-2.zarr"
    output_directory = "./zarr_volume"

    data = load_zarr_from_s3(s3_uri)
    summary = summarize_data(data)
    volume_path = save_full_volume(data, output_directory)
    write_metadata(output_directory, s3_uri, summary, volume_path)
