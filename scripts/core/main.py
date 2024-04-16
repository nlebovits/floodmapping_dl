import os
import argparse
import ee
import rasterio
from dotenv import load_dotenv
from google.cloud import storage

from utils.make_raw_dat import make_raw_dat
from utils.make_chips import make_chips
from utils.process_chips import process_chips
import argparse
import traceback
import pretty_errors



# Load and retrieve environment variables
load_dotenv()
cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT_NAME")
key_path = os.getenv("GOOGLE_CLOUD_KEY_PATH")

# Set GDAL environment configurations for GCS URLs
os.environ["GDAL_DISABLE_READDIR_ON_OPEN"] = "YES"
os.environ["CPL_VSIL_CURL_ALLOWED_EXTENSIONS"] = "tif"

client = storage.Client(project=cloud_project)

# Function to process flood data for specified countries
def main(countries):
    print("Initializing Earth Engine...")
    ee.Initialize(project=cloud_project)
    for place_name in countries:
        print("Processing data for", place_name, "...")
        
        snake_case_place_name = place_name.replace(" ", "_").lower()
        
        # Configure GCS bucket details for raw, chips, and processed data
        main_bucket_name = "hotspotstoplight_floodmapping"
        main_bucket = client.get_bucket(main_bucket_name)
        base_path = "deep_learning"

        # Define paths for raw, chips, and processed data within the main bucket
        raw_data_path = f"{base_path}/data/raw/{snake_case_place_name}"
        chips_data_path = f"{base_path}/data/chips/{snake_case_place_name}"
        processed_data_path = f"{base_path}/data/processed/{snake_case_place_name}"

        # Create raw data
        make_raw_dat(place_name, main_bucket, raw_data_path)
        
        # Chip the raw data
        make_chips(main_bucket, raw_data_path, chips_data_path)

        # Process the chips
        #process_chips(main_bucket, chips_data_path, main_bucket, processed_data_path)

        # Save the processed data
        # unclear if we need this or nor, given the structure of the previous sections
        # save_processed_data(processed_bucket, processed_bucket_path)


if __name__ == "__main__":
    print("Script is running")
    try:
        parser = argparse.ArgumentParser(description="Process flood data for given countries.")
        parser.add_argument("countries", metavar="Country", type=str, nargs="+", help="A list of countries to process")

        args = parser.parse_args()
        print("Countries to process:", args.countries)  # Debug print

        main(args.countries)
    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()

# Future code enhancements and tasks
# 1) Scale and one-hot encode raw data to create `processed` data; save to `processed` bucket in GCS
# 2) Train a model using the processed data and save to a `model` bucket in GCS
# 3) Evaluate the model using processed data and save results to an `evaluation` bucket in GCS
# 4) Use the model to make predictions for Costa Rica and save to a `prediction` bucket in GCS
