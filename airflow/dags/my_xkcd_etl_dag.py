import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from fetch_data import fetch_xkcd_data, clean_and_optimize_data, export_to_mongodb
from hdfs import InsecureClient
from pymongo import MongoClient

# Function to clear previously written files in HDFS
def clear_hdfs_files():
    hdfs_client = InsecureClient('http://localhost:9870', user='hdfs')
    paths = ['/user/hdfs/xkcd_data.json', '/user/hdfs/xkcd_cleaned_data.json']
    for path in paths:
        if hdfs_client.status(path, strict=False):
            hdfs_client.delete(path)
            print(f"Deleted {path} from HDFS")

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