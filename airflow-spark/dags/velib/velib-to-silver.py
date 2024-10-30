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
        dag_id="spark-app-velib-to-silver", 
        description="This DAG runs a Pyspark app that uses modules.",
        default_args=default_args, 
        schedule_interval=timedelta(1)
    )

start = DummyOperator(task_id="start", dag=dag)

spark_job = SparkSubmitOperator(
    task_id="velib-to-silver",
    application="/usr/local/spark/app/spark-app-velib-to-silver.py", # Spark application path created in airflow and spark cluster
    name="spark-app-velib-to-silver",
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