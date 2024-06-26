from dotenv import load_dotenv

import os
import numpy as np
from rasterio.io import MemoryFile
from google.cloud import storage
import tempfile
import warnings

# Load environment variables
load_dotenv()
cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT_NAME")
key_path = os.getenv("GOOGLE_CLOUD_KEY_PATH")

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = "tif"

# Initialize Google Cloud Storage client
client = storage.Client(project=cloud_project)

def process_chips(bucket, input_path_prefix, output_path_prefix, encoder=None):
    blobs = bucket.list_blobs(prefix=input_path_prefix)
    arrays = []
    masks_to_save = []

    for blob in blobs:
        if blob.name.endswith('.tif'):
            #print(f"Processing blob: {blob.name}")
            with MemoryFile(blob.download_as_bytes()) as memfile:
                with memfile.open(driver='GTiff') as src:
                    array = src.read()
                    # Check and ensure the array dimensions
                    if array.shape[1] != 512 or array.shape[2] != 512:
                        print(f"Skipping file {blob.name}, incorrect dimensions {array.shape}")
                        continue
                    # Check if the mask has any flooded pixels
                    mask = array[-1, :, :]
                    if np.any(mask == 1):
                        arrays.append(array)
                        masks_to_save.append(mask)

    if arrays:
        num_bands, height, width = arrays[0].shape
        num_files = len(arrays)
        print(f"Found {num_files} files with shape {num_bands} bands, {height}x{width} pixels.")

        all_arrays = np.stack(arrays, axis=0)
        masks = np.stack(masks_to_save, axis=0)

        # Process and encode the data
        landcover_data = all_arrays[:, 1, :, :].reshape(-1, 1)
        if encoder is None:
            from sklearn.preprocessing import OneHotEncoder
            encoder = OneHotEncoder(sparse_output=False)
        landcover_encoded = encoder.fit_transform(landcover_data).reshape(num_files, height, width, -1)
        landcover_encoded = np.transpose(landcover_encoded, (0, 3, 1, 2))
        print("Data encoding complete.")

        # Excluding specific bands
        all_arrays = np.delete(all_arrays, [1, -1], axis=1)

        # Scaling
        min_vals = np.nanmin(all_arrays, axis=(0, 2, 3))
        max_vals = np.nanmax(all_arrays, axis=(0, 2, 3))
        
        # Ensure min and max are broadcastable to the shape of all_arrays
        min_vals = min_vals[:, np.newaxis, np.newaxis]
        max_vals = max_vals[:, np.newaxis, np.newaxis]
        
        # Calculate the range and adjust zeros before any division attempt
        range_vals = max_vals - min_vals
        small_value = 1e-10
        range_vals[range_vals == 0] = small_value  # Prevent division by zero

        # Normalize the data
        try:
            all_arrays = (all_arrays - min_vals) / range_vals
            print("Data scaling complete.")
        except RuntimeWarning:
            print("Unexpected issue occurred during scaling.")

        # Concatenate scaled images with encoded land cover
        all_arrays = np.concatenate([all_arrays, landcover_encoded], axis=1)
        masks = masks[:, np.newaxis, :, :]

        print("Data concatenation complete. Saving processed data...")
        save_to_gcs(bucket, all_arrays, masks, output_path_prefix, 'processed_data/images.npy', 'processed_data/masks.npy')
        print("Data saved successfully.")
        

def save_to_gcs(bucket, images_array, masks_array, output_path_prefix, images_blob_name, masks_blob_name):
    """Helper function to save arrays to GCS using temporary files."""
    with tempfile.NamedTemporaryFile(delete=False) as images_temp, tempfile.NamedTemporaryFile(delete=False) as masks_temp:
        np.save(images_temp, images_array)
        np.save(masks_temp, masks_array)

        images_temp.close()
        masks_temp.close()

        images_blob = bucket.blob(f"{output_path_prefix}/{images_blob_name}")
        masks_blob = bucket.blob(f"{output_path_prefix}/{masks_blob_name}")

        images_blob.upload_from_filename(images_temp.name, content_type='application/octet-stream')
        masks_blob.upload_from_filename(masks_temp.name, content_type='application/octet-stream')

        # Remove the temporary files after uploading
        os.remove(images_temp.name)
        os.remove(masks_temp.name)



