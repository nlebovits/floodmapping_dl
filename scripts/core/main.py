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
import time 
import traceback
import pretty_errors



# Load and retrieve environment variables
load_dotenv()
cloud_project = os.getenv("GOOGLE_CLOUD_PROJECT_NAME")
key_path = os.getenv("GOOGLE_CLOUD_KEY_PATH")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = key_path

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
        process_chips(main_bucket, chips_data_path, processed_data_path)


if __name__ == "__main__":
    print("Script is running")
    start_time = time.time()  # Start the timer
    try:
        parser = argparse.ArgumentParser(description="Process flood data for given countries.")
        parser.add_argument("countries", metavar="Country", type=str, nargs="+", help="A list of countries to process")

        args = parser.parse_args()
        print("Countries to process:", args.countries)  # Debug print

        main(args.countries)
    except Exception as e:
        print("An error occurred:", e)
        traceback.print_exc()
    finally:
        elapsed_time = time.time() - start_time  # Calculate elapsed time
        print(f"Total execution time: {elapsed_time:.2f} seconds")  # Print the elapsed time

# Future code enhancements and tasks
# 1) Train a model using the processed data and save to a `model` bucket in GCS
# 2) Evaluate the model using processed data and save results to an `evaluation` bucket in GCS
# 3) Use the model to make predictions for Costa Rica and save to a `prediction` bucket in GCS
