import os
import numpy as np
from rasterio import windows
from itertools import product
from google.cloud import storage
from dotenv import load_dotenv
import rasterio
from rasterio import windows

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
# Function to process and save chipped tiles
def make_chips(bucket_name, input_path_prefix, output_path_prefix):
    print(bucket_name, input_path_prefix, output_path_prefix)
    bucket = storage.Client().get_bucket(bucket_name)  # Retrieve the bucket by name
    blobs = bucket.list_blobs(prefix=input_path_prefix)

    tiles_filenames = []  # This list will store the filenames of all saved tiles

    for blob in blobs:
        if blob.name.endswith('.tif'):
            print(f"Processing file: {blob.name}")
            
            # Download blob content as string
            data = blob.download_as_string()
            
            # Open the blob's content with rasterio
            with rasterio.io.MemoryFile(data) as memfile:
                with memfile.open() as src:
                    tile_index = 0
                    for window, transform in get_tiles(src):
                        # Read band 2 for the current window (tile)
                        band2 = src.read(2, window=window)

                        # Check if band 2 has any non-zero values
                        if np.any(band2 != 0):  # If true, proceed to process and save the tile
                            tile = src.read(window=window)  # Read all bands for the window

                            # Check and pad each band if the image is not 512x512
                            padded_tile = np.zeros((tile.shape[0], 512, 512), dtype=tile.dtype)
                            for i in range(tile.shape[0]):  # Iterate over each band in the tile
                                band = tile[i, :, :]
                                padded_band = np.pad(band, 
                                                    ((0, max(0, 512 - band.shape[0])), 
                                                    (0, max(0, 512 - band.shape[1]))), 
                                                    mode='constant', constant_values=0)
                                padded_tile[i, :, :] = padded_band

                            meta = src.meta.copy()
                            meta.update({
                                "driver": "GTiff",
                                "height": 512,
                                "width": 512,
                                "transform": transform
                            })

                            # Generate a unique filename for the tile
                            unique_part = blob.name.split('/')[-1].replace('.tif', '')
                            filename = f'{unique_part}_tile_{tile_index}.tif'
                            full_path = os.path.join(output_path_prefix, filename)

                            # Upload TIFF data to Google Cloud Storage
                            blob_path = os.path.join(output_path_prefix, filename)
                            bucket.blob(blob_path).upload_from_string(memfile.read(), content_type='image/tiff')

                            tiles_filenames.append(full_path)  # Append the full path to the list
                        tile_index += 1
            print(f"Finished processing {blob.name}")

