import os
import numpy as np
from rasterio import windows
from itertools import product
from google.cloud import storage
from dotenv import load_dotenv
import rasterio
from rasterio import windows
from rasterio.io import MemoryFile

# Load environment variables
load_dotenv()
cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT_NAME")
key_path = os.getenv("GOOGLE_CLOUD_KEY_PATH")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = "tif"

# Initialize Google Cloud Storage client
client = storage.Client(project=cloud_project)

# Function to get tiles from a dataset
def get_tiles(ds, width=512, height=512):
    nols, nrows = ds.meta['width'], ds.meta['height']
    offsets = product(range(0, nols, width), range(0, nrows, height))
    big_window = windows.Window(col_off=0, row_off=0, width=nols, height=nrows)
    for col_off, row_off in offsets:
        window = windows.Window(col_off=col_off, row_off=row_off, width=width, height=height).intersection(big_window)
        transform = windows.transform(window, ds.transform)
        yield window, transform

# Function to process and save chipped tiles
def make_chips(bucket_name, input_path_prefix, output_path_prefix):
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=input_path_prefix)

    for blob in blobs:
        if blob.name.endswith('.tif'):
            data = blob.download_as_bytes()

            with MemoryFile(data) as memfile:
                with memfile.open() as src:
                    for window, transform in get_tiles(src):
                        tile = src.read(window=window)
                        if np.any(tile != 0):  # Check if there's any non-zero data in the tile
                            meta = src.meta.copy()
                            meta.update({
                                "driver": "GTiff",
                                "height": window.height,
                                "width": window.width,
                                "transform": transform
                            })

                            # Create a new MemoryFile for each tile
                            with MemoryFile() as tile_memfile:
                                with tile_memfile.open(**meta) as tile_dst:
                                    tile_dst.write(tile)

                                # Save to Cloud Storage
                                tile_blob = bucket.blob(os.path.join(output_path_prefix, f'{window.col_off}_{window.row_off}.tif'))
                                tile_blob.upload_from_string(tile_memfile.read(), content_type='image/tiff')
                                # print(f"Tile saved to: {tile_blob.name}")  # Add this line to print the path where the tile is saved
            print(f"Finished processing {blob.name}")

