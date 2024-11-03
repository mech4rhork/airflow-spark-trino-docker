import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp, concat, substring, lit
from pyspark.sql import DataFrame
import trino
from modules import sparkDatalakeUtils

print("######################################")
print("0. Create spark session")
print("######################################")

spark = spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("######################################")
print("1. Extract from object storage")
print("######################################")

input_path = f"{os.getenv('S3_INPUT_PATH')}/part_day={os.environ['PART_DAY']}"
df = spark.read.parquet(input_path).cache()

print("######################################")
print("1.5. Declare functions")
print("######################################")

import pyspark.sql.functions as F
from pyspark.sql.window import Window

def calc_turnover_rate_by_station_10min(input_df):
    windowSpec  = Window.partitionBy("stationcode", "ten_minute_interval").orderBy("polldate_timestamp_minute")

    df_transformed = input_df#.dropDuplicates(["stationcode", "polldate_timestamp_minute"])
    df_transformed = (
        df_transformed
        .withColumn("ten_minute_interval", F.substring("polldate", 1, 15))
        .withColumn("numbikesavailable_abs_lag_diff",
            F.abs(F.col("numbikesavailable") - F.lag("numbikesavailable", 1).over(windowSpec)))
        )
    df_transformed = (
        df_transformed
        .groupBy("stationcode", "ten_minute_interval")
        .agg(
            F.sum(F.col("numbikesavailable_abs_lag_diff")).alias("turnover_rate_10min"),
            F.first(F.col("name")).alias("name"),
            F.first(F.col("lat")).alias("lat"),
            F.first(F.col("lon")).alias("lon"),
            F.first(F.col("capacity")).alias("capacity"),
            F.first(F.col("part_day")).alias("part_day"),
            F.first(F.col("nom_arrondissement_communes")).alias("nom_arrondissement_communes"),
            F.first(F.col("code_insee_commune")).alias("code_insee_commune"),
            F.first(F.col("numero_departement")).alias("numero_departement"),
            F.first(F.col("polldate_timestamp_minute")).alias("polldate_timestamp_minute")
        )
        .filter(F.col("turnover_rate_10min") < 500)
        .na.fill({'turnover_rate_10min': .0})
    )
    df_transformed = df_transformed.select(*(sorted(df_transformed.columns)))
    return df_transformed

def calc_summary_stats_by_station_1hour(input_df):
    df_transformed = input_df
    df_transformed = (
        df_transformed
        .withColumn("one_hour_interval", F.substring("polldate", 1, 13))
        .groupBy("stationcode", "one_hour_interval")
        .agg(
            F.avg(F.col("numbikesavailable")).alias("numbikesavailable_avg_1hour"),
            F.min(F.col("numbikesavailable")).alias("numbikesavailable_min_1hour"),
            F.max(F.col("numbikesavailable")).alias("numbikesavailable_max_1hour"),
            F.avg(F.col("fill_ratio")).alias("fill_ratio_avg_1hour"),
            F.min(F.col("fill_ratio")).alias("fill_ratio_min_1hour"),
            F.max(F.col("fill_ratio")).alias("fill_ratio_max_1hour"),
            F.first(F.col("name")).alias("name"),
            F.first(F.col("lat")).alias("lat"),
            F.first(F.col("lon")).alias("lon"),
            F.first(F.col("capacity")).alias("capacity"),
            F.first(F.col("part_day")).alias("part_day"),
            F.first(F.col("nom_arrondissement_communes")).alias("nom_arrondissement_communes"),
            F.first(F.col("code_insee_commune")).alias("code_insee_commune"),
            F.first(F.col("numero_departement")).alias("numero_departement"),
            F.first(F.col("polldate_timestamp_minute")).alias("polldate_timestamp_minute")
        )
        .na.fill({
            'numbikesavailable_avg_1hour': .0,
            'numbikesavailable_min_1hour': .0,
            'numbikesavailable_max_1hour': .0,
            'fill_ratio_avg_1hour': .0,
            'fill_ratio_min_1hour': .0,
            'fill_ratio_max_1hour': .0
        })
    )
    df_transformed = df_transformed.select(*(sorted(df_transformed.columns)))
    return df_transformed


def calc_summary_stats_1hour(input_df):
    df_transformed = input_df
    df_transformed = (
        df_transformed
        .withColumn("one_hour_interval", F.substring("polldate", 1, 13))
        .groupBy("one_hour_interval")
        .agg(
            F.avg(F.col("numbikesavailable")).alias("numbikesavailable_avg_1hour"),
            F.min(F.col("numbikesavailable")).alias("numbikesavailable_min_1hour"),
            F.max(F.col("numbikesavailable")).alias("numbikesavailable_max_1hour"),
            F.avg(F.col("fill_ratio")).alias("fill_ratio_avg_1hour"),
            F.min(F.col("fill_ratio")).alias("fill_ratio_min_1hour"),
            F.max(F.col("fill_ratio")).alias("fill_ratio_max_1hour"),
            F.first(F.col("name")).alias("name"),
            F.first(F.col("lat")).alias("lat"),
            F.first(F.col("lon")).alias("lon"),
            F.first(F.col("capacity")).alias("capacity"),
            F.first(F.col("part_day")).alias("part_day"),
            F.first(F.col("nom_arrondissement_communes")).alias("nom_arrondissement_communes"),
            F.first(F.col("code_insee_commune")).alias("code_insee_commune"),
            F.first(F.col("numero_departement")).alias("numero_departement"),
            F.first(F.col("polldate_timestamp_minute")).alias("polldate_timestamp_minute")
        )
        .na.fill({
            'numbikesavailable_avg_1hour': .0,
            'numbikesavailable_min_1hour': .0,
            'numbikesavailable_max_1hour': .0,
            'fill_ratio_avg_1hour': .0,
            'fill_ratio_min_1hour': .0,
            'fill_ratio_max_1hour': .0
        })
    )
    df_transformed = df_transformed.select(*(sorted(df_transformed.columns)))
    return df_transformed

def write_parquet(input_df, dataset_name):
    (
        input_df.write
        .format("parquet")
        .mode("overwrite")
        .save(f"{os.getenv('S3_OUTPUT_PATH')}/{dataset_name}/part_day={os.getenv('PART_DAY')}")
    )

import trino

def connect_to_trino(host='trino-coordinator', port=8080, user='trino'):
    return trino.dbapi.connect(host=host, port=port, user=user)

def create_table(input_df, dataset_name):
    conn = connect_to_trino()
    cur = conn.cursor()

    catalog, schema, table = 'minio', 'velib_gold', dataset_name
    partitioned_by = 'part_day'
    schema_location = os.environ["S3_OUTPUT_PATH"]
    external_location = f'{os.environ["S3_OUTPUT_PATH"]}/{dataset_name}'

    # List of queries to execute
    queries = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} WITH (location = '{schema_location}')",
        f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}",

        sparkDatalakeUtils.commonFunctions.generate_trino_create_table(input_df, catalog, schema, table, partitioned_by, external_location),

        f"USE {catalog}.{schema}",
        f"CALL system.sync_partition_metadata('{schema}', '{table}', 'ADD')",
        f"SELECT * FROM {catalog}.{schema}.{table} LIMIT 5"
    ]

    # Execute each query in the list
    for query in queries:
        try:
            cur.execute(query)
            # Check if the query is a SELECT query to fetch results
            if query.startswith("SELECT"):
                results = cur.fetchall()
                for row in results:
                    print(row)
            else:
                print(f"Executed: {query}")
        except Exception as e:
            raise Exception(f"Error executing query: {query}. Error: {e}")

    # Close the cursor and connection
    cur.close()
    conn.close()



dataset_transform_function_map = {
    'turnover_rate_by_station_10min': calc_turnover_rate_by_station_10min,
    'summary_stats_by_station_1hour': calc_summary_stats_by_station_1hour,
    'summary_stats_1hour': calc_summary_stats_1hour
}

for dataset_name, transform_function in dataset_transform_function_map.items():
    
    print("######################################")
    print(f"2. Transform (dataset_name={dataset_name})")
    print("######################################")
    df_transformed = transform_function(df)

    print("######################################")
    print(f"3. Load into object storage (dataset_name={dataset_name})")
    print("######################################")
    write_parquet(df_transformed, dataset_name)

    print("######################################")
    print(f"4. Update hive metastore (dataset_name={dataset_name})")
    print("######################################")
    create_table(df_transformed, dataset_name)






