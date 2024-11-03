from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# MODULES
###############################################
import trino
import boto3
from botocore.client import Config
import re
import time


def create_s3_client(endpoint_url="http://minio:9000", access_key="minio", secret_key="minio123"):
    """
    Creates and returns an S3 client with the given parameters.
    """
    return boto3.client(
        's3',
        endpoint_url=endpoint_url,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        config=Config(signature_version='s3v4')
    )

def get_part_day_values(s3_client, bucket_name, prefix=""):
    """
    Retrieves 'part_day' values from files under the given bucket path.
    The files are named with a pattern like 'YYYY-MM-DD.success'.
    
    Args:
        s3_client: The S3 client object.
        bucket_name: The name of the S3 bucket.
        prefix: The prefix path in the S3 bucket.
    
    Returns:
        A sorted list of unique 'part_day' values (dates in 'YYYY-MM-DD' format).
    """
    date_pattern = re.compile(r"(\d{4}-\d{2}-\d{2})\.success")
    part_day_values = set()
    
    # Use paginator to retrieve all objects under the specified bucket and prefix
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            # Extract the date from file name if it matches the pattern YYYY-MM-DD.success
            match = date_pattern.search(obj["Key"])
            if match:
                part_day_values.add(match.group(1))  # Add the date to the set

    return sorted(part_day_values)

def list_unique_dates(s3_client, bucket_name, prefix=""):
    """
    Lists unique date values (YYYY-MM-DD) from S3 keys in the specified bucket and prefix.
    """
    date_pattern = re.compile(r"\d{4}-\d{2}-\d{2}")
    unique_dates = set()
    paginator = s3_client.get_paginator("list_objects_v2")
    
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get("Contents", []):
            match = date_pattern.search(obj["Key"])  # Assigning match without walrus operator
            if match:  # Checking if match is not None
                unique_dates.add(match.group(0))

    return sorted(unique_dates)

def build_part_day_todo():
    s3_client = create_s3_client()
    bucket_name = "velib"

    # Retrieve part_day_done
    part_day_done = get_part_day_values(s3_client, bucket_name, "_tech/flags/job_success_gold")
    print("part_day_done =", part_day_done)

    # Retrieve unique dates
    part_day_all = list_unique_dates(s3_client, bucket_name, "silver")
    print("part_day_all =", part_day_all)

    # Calculate part_day_todo
    part_day_todo = [item for item in part_day_all if item not in part_day_done]
    print("part_day_todo =", part_day_todo)
    
    return sorted(part_day_todo)   

def write_object_to_minio(s3_client, bucket_name, object_key, data):
    """
    Writes an object to MinIO.

    Args:
        s3_client: The S3 client object.
        bucket_name: The name of the S3 bucket.
        object_key: The key (filename) for the object to be written.
        data: The data to be written (can be a string or bytes).
    """
    # If data is a string, encode it to bytes
    if isinstance(data, str):
        data = data.encode('utf-8')
    
    # Upload the object to the specified bucket and key
    s3_client.put_object(Bucket=bucket_name, Key=object_key, Body=data)
    print(f"Successfully uploaded {object_key} to bucket {bucket_name}.")

def write_job_success_flag(**kwargs):
    part_day_value = kwargs['part_day_value']
    s3_client = create_s3_client()
    write_object_to_minio(s3_client, "velib", f'_tech/flags/job_success_gold/{part_day_value}.success', "content")



###############################################
# Parameters
###############################################
# Initially empty part_day_values to be populated
part_day_values = []

###############################################
# DAG Definition
###############################################
now = datetime.now()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(now.year, now.month, now.day),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-app-velib-to-gold-PART_DAY", 
        description="todo",
        default_args=default_args, 
        schedule_interval='59 23 * * *'
    )

start = DummyOperator(task_id="start", dag=dag)
end = DummyOperator(task_id="end", dag=dag)


part_day_values = build_part_day_todo()

# Loop over each part_day_value and create a separate SparkSubmitOperator task
for part_day_value in part_day_values:
    spark_task = SparkSubmitOperator(
        task_id=f"spark_job_{part_day_value}",
        application="/usr/local/spark/app/spark-app-velib-to-gold-PART_DAY.py",
        name=f"spark-app-velib-to-gold-PART_DAY={part_day_value}",
        conn_id="spark_default",
        verbose=1,
        packages='org.apache.hadoop:hadoop-aws:3.2.0,org.apache.hadoop:hadoop-common:3.2.0',
        env_vars={
            "PART_DAY": part_day_value,
            "S3_INPUT_PATH": "s3a://velib/silver/velib-disponibilite-en-temps-reel",
            "S3_OUTPUT_PATH": "s3a://velib/gold"
        },
        conf={
            "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
            "spark.hadoop.fs.s3a.access.key": "minio",
            "spark.hadoop.fs.s3a.secret.key": "minio123",
            "spark.hadoop.fs.s3a.connection.ssl.enabled": "false",
            "spark.hadoop.fs.s3a.path.style.access": "true",
            "spark.hadoop.fs.s3a.attempts.maximum": "1",
            "spark.hadoop.fs.s3a.connection.establish.timeout": "5000",
            "spark.hadoop.fs.s3a.connection.timeout": "10000"
        },
        application_args=[],
        dag=dag
    )

    flag_success_task = PythonOperator(
        task_id=f'write_job_success_flag_{part_day_value}',
        python_callable=write_job_success_flag,
        op_kwargs={
            'part_day_value': part_day_value
        },
        dag=dag
    )

    start >> spark_task >> flag_success_task >> end