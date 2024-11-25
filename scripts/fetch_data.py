import os
import requests
import json
from pymongo import MongoClient
import time
from hdfs import InsecureClient

# Constants
XKCD_URL = "https://xkcd.com/{}/info.0.json"
BASE_URL = "https://xkcd.com/"
ENDPOINT = "/info.0.json"
START_COMIC = 1
END_COMIC = 10
RAW_DATA_DIR = "raw_data"
ENHANCED_DATA_FILE = "enhanced_data.json"
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
DB_NAME = "xkcdDB"
COLLECTION_NAME = "comics"
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:50070")
HDFS_RAW_DATA_DIR = "/user/raw_data"
HDFS_ENHANCED_DATA_FILE = "/user/enhanced_data.json"
HDFS_FINAL_DATA_DIR = "/user/final_data"

# Create directories if they don't exist
if not os.path.exists(RAW_DATA_DIR):
    os.makedirs(RAW_DATA_DIR)

# Initialize MongoDB client
client = MongoClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Initialize HDFS client
hdfs_client = InsecureClient(HDFS_URL)

# Function to clean and optimize raw data
def clean_and_optimize_data(raw_data):
    # Example of cleaning and optimizing data
    optimized_data = {
        "id": raw_data["num"],
        "title": raw_data["title"].strip(),
        "alt_text": raw_data["alt"].strip(),
        "image_url": raw_data["img"],
        "safe_title": raw_data.get("safe_title", "").strip(),
        "transcript": raw_data.get("transcript", "").strip(),
        "year": raw_data.get("year", ""),
        "month": raw_data.get("month", ""),
        "day": raw_data.get("day", "")
    }
    return optimized_data

# Function to fetch comic data from XKCD
def fetch_xkcd_data():
    enhanced_comics = []
    
    for comic_id in range(START_COMIC, END_COMIC + 1):
        url = XKCD_URL.format(comic_id)
        
        # Fetch comic data
        response = requests.get(url)
        
        print(f"Fetching comic {comic_id} from {url}")
        
        if response.status_code == 200:
            try:
                # Parse the JSON response
                current_comic = response.json()
                
                # Save raw data to local file
                raw_data_path = os.path.join(RAW_DATA_DIR, f"comic_{comic_id}.json")
                with open(raw_data_path, 'w') as raw_file:
                    json.dump(current_comic, raw_file, indent=4)

                # Save raw data to HDFS
                hdfs_raw_data_path = f"{HDFS_RAW_DATA_DIR}/comic_{comic_id}.json"
                with hdfs_client.write(hdfs_raw_data_path, encoding='utf-8') as hdfs_file:
                    json.dump(current_comic, hdfs_file, indent=4)

                # Clean and optimize the data
                optimized_comic = clean_and_optimize_data(current_comic)
                enhanced_comics.append(optimized_comic)

                # Save cleaned data to final HDFS directory
                hdfs_final_data_path = f"{HDFS_FINAL_DATA_DIR}/comic_{comic_id}.json"
                with hdfs_client.write(hdfs_final_data_path, encoding='utf-8') as hdfs_file:
                    json.dump(optimized_comic, hdfs_file, indent=4)

                # Insert into MongoDB
                export_to_mongodb(optimized_comic)
                
                # Sleep to avoid rate limiting
                time.sleep(1)
                
            except ValueError as e:
                print(f"Error parsing JSON for comic {comic_id}: {e}")
        else:
            print(f"Error fetching comic {comic_id}: {response.status_code}")
    
    # Save enhanced data to local file
    with open(ENHANCED_DATA_FILE, 'w') as enhanced_file:
        json.dump(enhanced_comics, enhanced_file, indent=4)

    # Save enhanced data to HDFS
    with hdfs_client.write(HDFS_ENHANCED_DATA_FILE, encoding='utf-8') as hdfs_file:
        json.dump(enhanced_comics, hdfs_file, indent=4)

# Function to export cleaned data to MongoDB
def export_to_mongodb(optimized_comic):
    collection.update_one(
        {"id": optimized_comic["id"]},
        {"$set": optimized_comic},
        upsert=True
    )

# Run the data fetching process
if __name__ == "__main__":
    fetch_xkcd_data()
    print("Data fetching and processing complete.")