import json
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
from hdfs import InsecureClient
from pymongo import MongoClient

# Your Python functions for fetching and cleaning data
def fetch_data():
    # Fetch data from XKCD and save it to HDFS
    url = 'https://xkcd.com/info.0.json'
    response = requests.get(url)
    data = response.json()

    hdfs_client = InsecureClient('http://localhost:9870', user='hdfs')
    with hdfs_client.write('/user/hdfs/xkcd_data.json', encoding='utf-8') as writer:
        json.dump(data, writer)

def clean_data():
    # Clean and process the raw data
    hdfs_client = InsecureClient('http://localhost:9870', user='hdfs')
    with hdfs_client.read('/user/hdfs/xkcd_data.json', encoding='utf-8') as reader:
        data = json.load(reader)

    # Example of cleaning: remove unwanted fields
    cleaned_data = {
        'num': data['num'],
        'title': data['title'],
        'img': data['img'],
        'alt': data['alt']
    }

    with hdfs_client.write('/user/hdfs/xkcd_cleaned_data.json', encoding='utf-8') as writer:
        json.dump(cleaned_data, writer)

def export_to_mongo():
    # Export the cleaned data to MongoDB
    client = MongoClient('mongodb://localhost:27017/')
    db = client['xkcd']
    collection = db['comics']

    hdfs_client = InsecureClient('http://localhost:9870', user='hdfs')
    with hdfs_client.read('/user/hdfs/xkcd_cleaned_data.json', encoding='utf-8') as reader:
        cleaned_data = json.load(reader)

    collection.insert_one(cleaned_data)

# Define the DAG
dag = DAG(
    'xkcd_etl',
    description='XKCD ETL Workflow',
    schedule_interval='@daily',  # Adjust as needed
    start_date=datetime(2023, 11, 24),  # Ensure this is a past date
    catchup=False
)

# Define the tasks
fetch_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data,
    dag=dag
)

export_task = PythonOperator(
    task_id='export_to_mongo',
    python_callable=export_to_mongo,
    dag=dag
)

# Set task dependencies
fetch_task >> clean_task >> export_task