import os
import numpy as np
import rasterio
from rasterio.io import MemoryFile
from google.cloud import storage
from sklearn.preprocessing import OneHotEncoder
import tempfile

def process_chips(input_bucket_name, input_path_prefix, output_bucket_name, encoder=None):
    # Initialize Google Cloud Storage client
    client = storage.Client()

    # Reference the input and output buckets
    input_bucket = client.get_bucket(input_bucket_name)
    output_bucket = client.get_bucket(output_bucket_name)

    # Initialize OneHotEncoder if not provided
    if encoder is None:
        encoder = OneHotEncoder(sparse=False)

    blobs = input_bucket.list_blobs(prefix=input_path_prefix)
    arrays = []

    for blob in blobs:
        if blob.name.endswith('.tif'):
            with MemoryFile(blob.download_as_bytes()) as memfile:
                with memfile.open() as src:
                    array = src.read()
                    arrays.append(array)

    if arrays:
        num_bands, height, width = arrays[0].shape
        num_files = len(arrays)
        all_arrays = np.stack(arrays, axis=0)

        # Process and encode the data
        masks = all_arrays[:, 6, :, :]
        landcover_data = all_arrays[:, 1, :, :].reshape(-1, 1)
        landcover_encoded = encoder.fit_transform(landcover_data).reshape(num_files, height, width, -1)
        landcover_encoded = np.transpose(landcover_encoded, (0, 3, 1, 2))

        # Excluding specific bands
        all_arrays = np.delete(all_arrays, [1, 6], axis=1)

        # Scaling
        min_vals = np.nanmin(all_arrays, axis=(0, 2, 3))
        max_vals = np.nanmax(all_arrays, axis=(0, 2, 3))
        all_arrays = (all_arrays - min_vals) / (max_vals - min_vals)

        # Concatenate scaled images with encoded land cover
        all_arrays = np.concatenate([all_arrays, landcover_encoded], axis=1)
        masks = masks[:, np.newaxis, :, :]

        # Save processed arrays to GCS
        save_to_gcs(client, all_arrays, masks, output_bucket, 'processed_data/images.npy', 'processed_data/masks.npy')

def save_to_gcs(client, images_array, masks_array, bucket, images_blob_name, masks_blob_name):
    """Helper function to save arrays to GCS using temporary files."""
    with tempfile.NamedTemporaryFile() as images_temp, tempfile.NamedTemporaryFile() as masks_temp:
        np.save(images_temp, images_array)
        np.save(masks_temp, masks_array)

        images_temp.seek(0)
        masks_temp.seek(0)

        images_blob = bucket.blob(images_blob_name)
        masks_blob = bucket.blob(masks_blob_name)

        images_blob.upload_from_filename(images_temp.name, content_type='application/octet-stream')
        masks_blob.upload_from_filename(masks_temp.name, content_type='application/octet-stream')


