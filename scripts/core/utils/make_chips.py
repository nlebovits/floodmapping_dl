import os
import numpy as np
import rasterio
from rasterio import windows
from itertools import product
from google.cloud import storage
from dotenv import load_dotenv
from google.colab import auth
from rasterio.io import MemoryFile

# Load environment variables
load_dotenv()
cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT_NAME")

# Authenticate user for Google Colab
auth.authenticate_user()

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
def make_chips(bucket_name, input_path_prefix, output_bucket_name):
    input_bucket = client.get_bucket(bucket_name)
    blobs = input_bucket.list_blobs(prefix=input_path_prefix)

    # Ensure GDAL is configured to work with GCS URLs
    os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
    os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = "tif"

    for blob in blobs:
        if blob.name.endswith('.tif'):
            # Determine country from the blob path
            country_name = blob.name.split('/')[2]  # Adjust the index based on your bucket structure
            output_dir = f'chips/{country_name}/'
            
            # Read blob into rasterio
            with MemoryFile(blob.download_as_bytes()) as memfile:
                with memfile.open() as src:
                    tile_index = 0
                    for window, transform in get_tiles(src):
                        tile = src.read(window=window)
                        if np.any(tile):
                            padded_tile = np.zeros((tile.shape[0], 512, 512), dtype=tile.dtype)
                            for i in range(tile.shape[0]):
                                band = tile[i, :, :]
                                padded_band = np.pad(band, ((0, max(0, 512 - band.shape[0])), 
                                                            (0, max(0, 512 - band.shape[1]))), 
                                                     mode='constant', constant_values=0)
                                padded_tile[i, :, :] = padded_band

                            meta = src.meta.copy()
                            meta.update({"driver": "GTiff", "height": 512, "width": 512, "transform": transform})
                            filename = f'{blob.name.replace(".tif", "")}_tile_{tile_index}.tif'
                            
                            # Save tiled image to the output bucket
                            output_bucket = client.get_bucket(output_bucket_name)
                            blob_path = output_dir + filename
                            output_blob = output_bucket.blob(blob_path)
                            with MemoryFile() as memfile:
                                with memfile.open('w', **meta) as memdst:
                                    memdst.write(padded_tile)
                                output_blob.upload_from_string(memfile.getvalue(), content_type='image/tiff')
                            tile_index += 1