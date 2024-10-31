from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"
spark_app_name = "velib-to-silver-gold"

def exec_bronze_to_silver_etl():
    exec_1_extract()
    exec_2_transform()
    exec_3_load()
    exec_4_update_hive_metastore()

# Define the default arguments
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),  # Start from one day ago
}

# Create the DAG
with DAG(
    dag_id='velib-to-silver-gold',
    default_args=default_args,
    schedule_interval='0 1 * * *',  # Schedule to run every day at 1 A.M.
    catchup=False,
) as dag:

    # Define the task
    fetch_and_upload = PythonOperator(
        task_id='exec_bronze_to_silver_etl',
        python_callable=exec_bronze_to_silver_etl,
    )

    fetch_and_upload




from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.contrib.operators.spark_submit_operator import SparkSubmitOperator
from datetime import datetime, timedelta

###############################################
# Parameters
###############################################
spark_master = "spark://spark:7077"

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
    "retries": 1,
    "retry_delay": timedelta(minutes=1)
}

dag = DAG(
        dag_id="spark-app-velib-to-silver-gold",
        description="This DAG runs a Pyspark app that uses modules.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="velib-to-silver-gold",
    application="/usr/local/spark/app/spark-app-velib-to-silver-gold.py", # Spark application path created in airflow and spark cluster
    name="spark-app-velib-to-silver-gold",
    conn_id="spark_default",
    verbose=1,
    packages='org.apache.hadoop:hadoop-aws:3.2.0,org.apache.hadoop:hadoop-common:3.2.0,io.trino:trino-jdbc:422',
    env_vars={
        "S3_INPUT_PATH": "s3a://velib/bronze/velib-disponibilite-en-temps-reel/*/*",
        "S3_OUTPUT_PATH": "s3a://velib/silver/velib-disponibilite-en-temps-reel"
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
    dag=dag)

end = DummyOperator(task_id="end", dag=dag)

start >> spark_job >> end