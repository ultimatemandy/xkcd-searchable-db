import os
import json
import time
import requests
from datetime import datetime
from pymongo import MongoClient
from hdfs import InsecureClient
from airflow import DAG
from airflow.operators.python import PythonOperator

# Constants
XKCD_URL = "https://xkcd.com/{}/info.0.json"
LATEST_COMIC_URL = "https://xkcd.com/info.0.json"
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:50070")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
RAW_DATA_DIR = os.getenv("RAW_DATA_DIR", "/tmp/raw_data")
DB_NAME = "xkcd"
COLLECTION_NAME = "comics"

# DAG Definition
default_args = {"owner": "airflow", "retries": 1, "retry_delay": 300}
dag = DAG(
    'xkcd_etl',
    description='ETL workflow for XKCD comics',
    schedule_interval='@daily',
    start_date=datetime(2023, 11, 24),
    catchup=False,
    default_args=default_args
)

# Functions
def clear_hdfs_files(**kwargs):
    try:
        client = InsecureClient(HDFS_URL)
        paths = ["/user/raw_data", "/user/final_data"]
        for path in paths:
            if client.status(path, strict=False):
                client.delete(path, recursive=True)
                print(f"Deleted {path} from HDFS.")
    except Exception as e:
        print(f"Error clearing HDFS: {e}")

def clear_mongo_collection(**kwargs):
    try:
        client = MongoClient(MONGO_URL)
        collection = client[DB_NAME][COLLECTION_NAME]
        result = collection.delete_many({})
        print(f"Cleared {result.deleted_count} documents from MongoDB.")
    except Exception as e:
        print(f"Error clearing MongoDB: {e}")

def fetch_xkcd_data(**kwargs):
    try:
        response = requests.get(LATEST_COMIC_URL)
        latest_comic = response.json()
        latest_comic_id = latest_comic['num']

        os.makedirs(RAW_DATA_DIR, exist_ok=True)
        enhanced_comics = []

        for comic_id in range(1, latest_comic_id + 1):
            url = XKCD_URL.format(comic_id)
            resp = requests.get(url)
            if resp.status_code == 200:
                comic = resp.json()
                raw_path = os.path.join(RAW_DATA_DIR, f"comic_{comic_id}.json")
                with open(raw_path, 'w') as raw_file:
                    json.dump(comic, raw_file)
                enhanced_comics.append(clean_and_optimize_data(comic))
                time.sleep(1)

        # Optionally write enhanced data to file
        enhanced_path = os.path.join(RAW_DATA_DIR, "enhanced_comics.json")
        with open(enhanced_path, 'w') as enhanced_file:
            json.dump(enhanced_comics, enhanced_file)

    except Exception as e:
        print(f"Error fetching XKCD data: {e}")

def clean_and_optimize_data(comic):
    return {
        "id": comic["num"],
        "title": comic["title"].strip(),
        "alt_text": comic["alt"].strip(),
        "image_url": comic["img"],
        "year": comic.get("year", ""),
        "month": comic.get("month", ""),
        "day": comic.get("day", "")
    }

def export_to_mongodb(**kwargs):
    try:
        client = MongoClient(MONGO_URL)
        collection = client[DB_NAME][COLLECTION_NAME]
        optimized_path = os.path.join(RAW_DATA_DIR, "enhanced_comics.json")
        with open(optimized_path, 'r') as file:
            comics = json.load(file)
            for comic in comics:
                collection.update_one({"id": comic["id"]}, {"$set": comic}, upsert=True)
    except Exception as e:
        print(f"Error exporting to MongoDB: {e}")

# Tasks
clear_hdfs = PythonOperator(task_id="clear_hdfs", python_callable=clear_hdfs_files, dag=dag)
clear_mongo = PythonOperator(task_id="clear_mongo", python_callable=clear_mongo_collection, dag=dag)
fetch_data = PythonOperator(task_id="fetch_data", python_callable=fetch_xkcd_data, dag=dag)
export_data = PythonOperator(task_id="export_data", python_callable=export_to_mongodb, dag=dag)

# Task Dependencies
clear_hdfs >> clear_mongo >> fetch_data >> export_data
