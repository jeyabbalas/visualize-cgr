import colorcet as cc
from dask import dataframe as dd
import datashader as ds
from google.cloud import storage
import numpy as np

import pathlib


def download_public_file(bucket_name, source_blob_name, destination_file_name):
    """Downloads a public blob from the bucket."""
    # bucket_name = "your-bucket-name"
    # source_blob_name = "storage-object-name"
    # destination_file_name = "local/path/to/file"

    storage_client = storage.Client.create_anonymous_client()

    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(source_blob_name)
    blob.download_to_filename(destination_file_name)

    print(
        "Downloaded public blob {} from bucket {} to {}.".format(
            source_blob_name, bucket.name, destination_file_name
        )
    )


def download_grch37_cgr_coords(output_csv):
    bucket_name = "chaos-game-representation-grch37"
    chromosomes = ["1", "2", "3", "4", "5",
                   "6", "7", "8", "9", "10",
                   "11", "12", "13", "14", "15",
                   "16", "17", "18", "19", "20",
                   "21", "22", "X", "Y", "M"]

    chunk_length = 1_000_000
    float64_bytes = 8
    num_coords = 4

    with open(output_csv, "a") as coords_file:
        for chr in chromosomes:
            destination_file_name = pathlib.Path("./scratch.bin")
            source_blob_name = f"chr{chr}/forward_backward_cgr.bin"
            download_public_file(bucket_name,
                                 source_blob_name,
                                 destination_file_name)

            with open(destination_file_name, "rb") as bin_file:
                while True:
                    msg = bin_file.read(chunk_length * float64_bytes * num_coords)
                    if not msg:
                        break;
                    msg_arr = np.frombuffer(msg, dtype=">f8").astype(np.float32).reshape((-1, 4))
                    np.savetxt(coords_file, msg_arr, fmt="%.8f")

            destination_file_name.unlink()


def convert_cgr_csv_to_parquet(csv_file, parquet_file):
    names = ["forward_x", "forward_y",
             "backward_x", "backward_y"]

    df = dd.read_csv(csv_file,
                     delimiter=" ",
                     header=None,
                     names=names,
                     dtype={"forward_x": np.float32,
                            "forward_y": np.float32,
                            "backward_x": np.float32,
                            "backward_y": np.float32})

    df.to_parquet(parquet_file,
                  engine="fastparquet",
                  write_metadata_file=False,
                  write_index=False,
                  compression="SNAPPY")

    csv_file.unlink()


def generate_cgr_plots(df_cgr_coords, forward_cgr_name, backward_cgr_name):
    # Forward CGR
    cvs = ds.Canvas(plot_width=1024, plot_height=1024,
                    x_range=(0.0,1.0), y_range=(0.0,1.0),
                    x_axis_type="linear", y_axis_type="linear")
    agg = cvs.points(df_cgr_coords, "forward_x", "forward_y", ds.count())
    img = ds.tf.shade(agg, cmap=list(reversed(cc.gray)))
    ds.utils.export_image(img, forward_cgr_name)

    # Backward CGR
    cvs = ds.Canvas(plot_width=1024, plot_height=1024,
                    x_range=(0.0,1.0), y_range=(0.0,1.0),
                    x_axis_type="linear", y_axis_type="linear")
    agg = cvs.points(df_cgr_coords, "backward_x", "backward_y", ds.count())
    img = ds.tf.shade(agg, cmap=list(reversed(cc.gray)))
    ds.utils.export_image(img, backward_cgr_name)


if __name__ == '__main__':
    output_csv = pathlib.Path("./grch37_cgr_coords.csv")
    output_parquet = pathlib.Path("./grch37_cgr_coords.parquet")

    download_grch37_cgr_coords(output_csv)
    convert_cgr_csv_to_parquet(output_csv, output_parquet)

    df = dd.read_parquet(output_parquet, engine="fastparquet")
    generate_cgr_plots(df, "grch37_forward_cgr", "grch37_backward_cgr")
