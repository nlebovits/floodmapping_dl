import os
import argparse
import ee
import rasterio
from dotenv import load_dotenv
from google.cloud import storage
from google.colab import auth

from utils.make_raw_data import make_raw_data
from utils.make_chips import make_chips
from utils.process_chips import process_chips

from data_utils.process_all_data import process_flood_data


# Load and retrieve environment variables
load_dotenv()
cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT_NAME")
key_path = os.getenv("GOOGLE_CLOUD_KEY_PATH")

# Set GDAL environment configurations for GCS URLs
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = "tif"

# Authenticate user for Google Colab and initialize Google Cloud Storage client
auth.authenticate_user()
client = storage.Client(project=cloud_project)

# Function to process flood data for specified countries
def main(countries):
    ee.Initialize(project=cloud_project)
    for place_name in countries:
        print("Processing data for", place_name, "...")
        
        # Configure GCS bucket details for raw, chips, and processed data
        raw_bucket_name = "raw"
        raw_bucket = client.get_bucket(raw_bucket_name)
        raw_bucket_path = f"data/{place_name}/raw"

        chips_bucket_name = "chips"
        chips_bucket_path = f"data/{place_name}/chips"

        processed_bucket_name = "processed"
        processed_bucket_path = f"data/{place_name}/processed"

        # Create raw data
        make_raw_data(place_name, raw_bucket, raw_bucket_path)

        # Chip the raw data
        make_chips(raw_bucket_name, raw_bucket_path, chips_bucket_name, chips_bucket_path)

        # Process the chips
        process_chips(chips_bucket_name, chips_bucket_path, processed_bucket_name, processed_bucket_path)

        # Save the processed data
        # unclear if we need this or nor, given the structure of the previous sections
        # save_processed_data(processed_bucket, processed_bucket_path)

# Future code enhancements and tasks
# 1) Scale and one-hot encode raw data to create `processed` data; save to `processed` bucket in GCS
# 2) Train a model using the processed data and save to a `model` bucket in GCS
# 3) Evaluate the model using processed data and save results to an `evaluation` bucket in GCS
# 4) Use the model to make predictions for Costa Rica and save to a `prediction` bucket in GCS
