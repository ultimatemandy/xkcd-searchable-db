import os
import requests
import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pymongo import MongoClient
import time
from hdfs import InsecureClient

# Constants
XKCD_URL = "https://xkcd.com/{}/info.0.json"
LATEST_COMIC_URL = "https://xkcd.com/info.0.json"
RAW_DATA_DIR = "raw_data"
ENHANCED_DATA_FILE = "enhanced_data.json"
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
DB_NAME = "xkcd"
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
try:
    hdfs_client = InsecureClient(HDFS_URL)
except Exception as e:
    print(f"Failed to connect to HDFS: {e}")
    hdfs_client = None

# Function to clean and optimize raw data
def clean_and_optimize_data(raw_data):
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
    response = requests.get(LATEST_COMIC_URL)
    latest_comic = response.json()
    latest_comic_id = latest_comic['num']
    
    enhanced_comics = []
    
    for comic_id in range(1, latest_comic_id + 1):
        url = XKCD_URL.format(comic_id)
        response = requests.get(url)
        print(f"Fetching comic {comic_id} from {url}")
        
        if response.status_code == 200:
            try:
                current_comic = response.json()
                raw_data_path = os.path.join(RAW_DATA_DIR, f"comic_{comic_id}.json")
                with open(raw_data_path, 'w') as raw_file:
                    json.dump(current_comic, raw_file, indent=4)

                if hdfs_client:
                    hdfs_raw_data_path = f"{HDFS_RAW_DATA_DIR}/comic_{comic_id}.json"
                    with hdfs_client.write(hdfs_raw_data_path, encoding='utf-8') as hdfs_file:
                        json.dump(current_comic, hdfs_file, indent=4)

                optimized_comic = clean_and_optimize_data(current_comic)
                enhanced_comics.append(optimized_comic)

                if hdfs_client:
                    hdfs_final_data_path = f"{HDFS_FINAL_DATA_DIR}/comic_{comic_id}.json"
                    with hdfs_client.write(hdfs_final_data_path, encoding='utf-8') as hdfs_file:
                        json.dump(optimized_comic, hdfs_file, indent=4)

                export_to_mongodb(optimized_comic)
                time.sleep(1)
                
            except ValueError as e:
                print(f"Error parsing JSON for comic {comic_id}: {e}")
        else:
            print(f"Error fetching comic {comic_id}: {response.status_code}")
    
    with open(ENHANCED_DATA_FILE, 'w') as enhanced_file:
        json.dump(enhanced_comics, enhanced_file, indent=4)

    if hdfs_client:
        with hdfs_client.write(HDFS_ENHANCED_DATA_FILE, encoding='utf-8') as hdfs_file:
            json.dump(enhanced_comics, hdfs_file, indent=4)

# Function to export cleaned data to MongoDB
def export_to_mongodb(optimized_comic):
    result = collection.update_one(
        {"id": optimized_comic["id"]},
        {"$set": optimized_comic},
        upsert=True
    )
    print(f"Inserted/Updated comic {optimized_comic['id']} in MongoDB. Matched: {result.matched_count}, Modified: {result.modified_count}")

# Function to clear previously written files in HDFS
def clear_hdfs_files():
    if hdfs_client:
        paths = ['/user/hdfs/xkcd_data.json', '/user/hdfs/xkcd_cleaned_data.json']
        for path in paths:
            if hdfs_client.status(path, strict=False):
                hdfs_client.delete(path)
                print(f"Deleted {path} from HDFS")
    else:
        print("HDFS client is not available.")

# Function to clear the MongoDB collection
def clear_mongo_collection():
    client = MongoClient('mongodb://localhost:27017/')
    db = client['xkcd']
    collection = db['comics']
    result = collection.delete_many({})
    print(f"Cleared {result.deleted_count} documents from the MongoDB collection.")

# Define the DAG
dag = DAG(
    'xkcd_etl',
    description='XKCD ETL Workflow',
    schedule_interval='@daily',  # Adjust as needed
    start_date=datetime(2023, 11, 24),  # Ensure this is a past date
    catchup=False
)

# Define the tasks
clear_hdfs_task = PythonOperator(
    task_id='clear_hdfs_files',
    python_callable=clear_hdfs_files,
    dag=dag
)

clear_mongo_task = PythonOperator(
    task_id='clear_mongo_collection',
    python_callable=clear_mongo_collection,
    dag=dag
)

fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_xkcd_data,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_and_optimize_data,
    dag=dag
)

export_task = PythonOperator(
    task_id='export_to_mongo',
    python_callable=export_to_mongodb,
    dag=dag
)

# Set task dependencies
clear_hdfs_task >> clear_mongo_task >> fetch_task >> clean_task >> export_task