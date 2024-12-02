from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import requests
import json
from pymongo import MongoClient
from hdfs import InsecureClient
from pyspark.sql import SparkSession
import time

# Constants
XKCD_URL = "https://xkcd.com/{}/info.0.json"
RAW_DATA_DIR = "raw_data"
ENHANCED_DATA_FILE = "enhanced_data.json"
MONGO_URL = os.getenv("MONGO_URL", "mongodb://localhost:27017/")
DB_NAME = "xkcd"
COLLECTION_NAME = "comics"
HDFS_URL = os.getenv("HDFS_URL", "http://localhost:50070")
HDFS_RAW_DATA_DIR = "/user/raw_data"
HDFS_FINAL_DATA_DIR = "/user/final_data"
MAX_COMIC_ID = 400  # Fetch comics up to the 400th one

# Set the SPARK_HOME environment variable
os.environ["SPARK_HOME"] = "/opt/homebrew/opt/apache-spark"
os.environ["PATH"] = os.environ["SPARK_HOME"] + "/bin:" + os.environ["PATH"]
os.environ["PYSPARK_PYTHON"] = "python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "python3"

# Create directories if they don't exist
def create_directories():
    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)
    print(f"Directory {RAW_DATA_DIR} is ready.")

# Initialize MongoDB client
client = MongoClient(MONGO_URL)
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

# Initialize HDFS client
hdfs_client = InsecureClient(HDFS_URL)

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

# Function to fetch comics up to the 400th one from XKCD
def fetch_comics():
    for comic_id in range(1, MAX_COMIC_ID + 1):
        url = XKCD_URL.format(comic_id)
        print(f"Fetching comic from URL: {url}")
        start_time = time.time()
        try:
            response = requests.get(url, timeout=30)  # Increase the timeout to 30 seconds
            response.raise_for_status()  # Raise an exception for HTTP errors
            current_comic = response.json()
            raw_data_path = os.path.join(RAW_DATA_DIR, f"comic_{comic_id}.json")
            with open(raw_data_path, 'w') as raw_file:
                json.dump(current_comic, raw_file, indent=4)
            end_time = time.time()
            print(f"Fetched comic {comic_id} in {end_time - start_time} seconds")
            print(f"Comic data saved to {raw_data_path}")
        except requests.exceptions.RequestException as e:
            end_time = time.time()
            print(f"Error fetching comic {comic_id}: {e}")
            print(f"Time taken before error: {end_time - start_time} seconds")
        except ValueError as e:
            end_time = time.time()
            print(f"Error parsing JSON for comic {comic_id}: {e}")
            print(f"Time taken before error: {end_time - start_time} seconds")
        except Exception as e:
            end_time = time.time()
            print(f"Unexpected error for comic {comic_id}: {e}")
            print(f"Time taken before error: {end_time - start_time} seconds")

# Function to store fetched data in a directory
def store_fetched_data():
    print(f"Storing fetched data in directory: {RAW_DATA_DIR}")
    # This function doesn't need to do anything since the data is already stored in the fetch_comics function
    pass

# Function to upload raw data to HDFS
def upload_raw_data_to_hdfs():
    for comic_id in range(1, MAX_COMIC_ID + 1):
        raw_data_path = os.path.join(RAW_DATA_DIR, f"comic_{comic_id}.json")
        if os.path.exists(raw_data_path):
            with open(raw_data_path, 'r') as raw_file:
                comic = json.load(raw_file)
            hdfs_raw_data_path = f"{HDFS_RAW_DATA_DIR}/comic_{comic_id}.json"
            with hdfs_client.write(hdfs_raw_data_path, encoding='utf-8') as hdfs_file:
                json.dump(comic, hdfs_file, indent=4)
            print(f"Raw data for comic {comic_id} uploaded to HDFS.")

# Function to clean and optimize data using Spark within HDFS
def clean_data_within_hdfs():
    spark = SparkSession.builder.appName("XKCDCleaner").getOrCreate()
    
    # Read raw data from HDFS
    raw_data_df = spark.read.json(f"{HDFS_RAW_DATA_DIR}/*.json")
    
    # Clean and optimize data
    optimized_data_df = raw_data_df.selectExpr(
        "num as id",
        "title",
        "alt as alt_text",
        "img as image_url",
        "safe_title",
        "transcript",
        "year",
        "month",
        "day"
    )
    
    # Write cleaned data back to HDFS
    optimized_data_df.write.mode("overwrite").json(HDFS_FINAL_DATA_DIR)
    
    print("Cleaned data uploaded to HDFS using Spark.")
    spark.stop()

# Function to export cleaned data to MongoDB
def export_to_mongodb_task():
    spark = SparkSession.builder.appName("XKCDToMongoDB").getOrCreate()
    
    # Read cleaned data from HDFS
    cleaned_data_df = spark.read.json(HDFS_FINAL_DATA_DIR)
    
    # Convert to Pandas DataFrame
    cleaned_data_pd = cleaned_data_df.toPandas()
    
    # Insert into MongoDB
    for _, row in cleaned_data_pd.iterrows():
        result = collection.update_one(
            {"id": row["id"]},
            {"$set": row.to_dict()},
            upsert=True
        )
        print(f"Inserted/Updated comic {row['id']} in MongoDB. Matched: {result.matched_count}, Modified: {result.modified_count}")
    
    spark.stop()

# Function to test HDFS connection
def test_hdfs_connection():
    try:
        os.system("hdfs dfs -ls /")
        print("HDFS connection successful.")
    except Exception as e:
        print(f"HDFS connection failed: {e}")

# Function to clear HDFS
def clear_hdfs():
    try:
        os.system("hdfs dfs -rm -r /user/raw_data")
        os.system("hdfs dfs -rm -r /user/final_data")
        print("HDFS cleared successfully.")
    except Exception as e:
        print(f"Failed to clear HDFS: {e}")

# Function to clear MongoDB
def clear_mongodb():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        db = client['xkcd']
        db.drop_collection('comics')
        print("MongoDB cleared successfully.")
    except Exception as e:
        print(f"Failed to clear MongoDB: {e}")

# Function to test MongoDB connection
def test_mongodb_connection():
    try:
        client = MongoClient("mongodb://localhost:27017/")
        client.server_info()  # Trigger a call to the server to check the connection
        print("MongoDB connection successful.")
    except Exception as e:
        print(f"MongoDB connection failed: {e}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'XKCD-ETL-DAG',
    default_args=default_args,
    description='ETL worfklow to fetch XKCD comics',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
)

# Task 1: Test HDFS connection
test_hdfs_task = PythonOperator(
    task_id='test_hdfs_connection',
    python_callable=test_hdfs_connection,
    dag=dag,
)

# Task 2: Test MongoDB connection
test_mongodb_task = PythonOperator(
    task_id='test_mongodb_connection',
    python_callable=test_mongodb_connection,
    dag=dag,
)

# Task 3: Clear HDFS
clear_hdfs_task = PythonOperator(
    task_id='clear_hdfs',
    python_callable=clear_hdfs,
    dag=dag,
)

# Task 4: Clear MongoDB
clear_mongodb_task = PythonOperator(
    task_id='clear_mongodb',
    python_callable=clear_mongodb,
    dag=dag,
)

# Task 5: Fetch comics up to the 400th one from XKCD API
fetch_comics_task = PythonOperator(
    task_id='fetch_comics',
    python_callable=fetch_comics,
    dag=dag,
)

# Task 6: Store fetched data in a directory
store_fetched_data_task = PythonOperator(
    task_id='store_fetched_data',
    python_callable=store_fetched_data,
    dag=dag,
)

# Task 7: Upload raw data to HDFS
upload_raw_data_to_hdfs_task = PythonOperator(
    task_id='upload_raw_data_to_hdfs',
    python_callable=upload_raw_data_to_hdfs,
    dag=dag,
)

# Task 8: Clean data within HDFS using Spark
clean_data_within_hdfs_task = PythonOperator(
    task_id='clean_data_within_hdfs',
    python_callable=clean_data_within_hdfs,
    dag=dag,
)

# Task 9: Export cleaned data to MongoDB
export_to_mongodb_task = PythonOperator(
    task_id='export_to_mongodb_task',
    python_callable=export_to_mongodb_task,
    dag=dag,
)

# Define task dependencies
test_hdfs_task >> clear_hdfs_task >> fetch_comics_task >> store_fetched_data_task >> upload_raw_data_to_hdfs_task >> clean_data_within_hdfs_task >> export_to_mongodb_task
test_mongodb_task >> clear_mongodb_task >> fetch_comics_task