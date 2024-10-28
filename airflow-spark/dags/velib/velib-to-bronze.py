from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import boto3
from botocore.client import Config
import json
import datetime

def fetch_from_api_and_upload_to_bucket():
    now = datetime.datetime.now(datetime.timezone.utc)
    timestamp_day = now.strftime("%Y-%m-%d")
    timestamp_minute = now.strftime("%Y-%m-%d-%H%M")

    dataset_name = 'velib-disponibilite-en-temps-reel'
    
    url = f"https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/{dataset_name}/exports/json"
    minio_url = "http://minio:9000"
    access_key = "minio"
    secret_key = "minio123"
    bucket_name = "velib"
    object_key = f"bronze/{dataset_name}/{timestamp_day}/{timestamp_minute}.json"

    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad responses
    print(len(response.json()))
    print(response.json()[0])
    
    s3_client = boto3.client(
        's3',
        endpoint_url=minio_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )
    object_content = json.dumps(response.json())
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=object_content)
    print(f"File '{object_key}' has been uploaded to bucket '{bucket_name}'.")

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Start from one day ago
}

# Create the DAG
with DAG(
    dag_id='velib-to-bronze',
    default_args=default_args,
    schedule_interval='* * * * *',  # Schedule to run every minute
    catchup=False,
) as dag:

    # Define the task
    fetch_and_upload = PythonOperator(
        task_id='fetch_from_api_and_upload_to_bucket',
        python_callable=fetch_from_api_and_upload_to_bucket,
    )

    fetch_and_upload
