import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import trino

print("######################################")
print("0. Create spark session")
print("######################################")

spark = spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("######################################")
print("1. Extract from object storage")
print("######################################")

df = spark.read.json(os.getenv("S3_INPUT_PATH"))

print("######################################")
print("2. Transform")
print("######################################")

df_transformed = (
    df
    .withColumn("fill_ratio", F.col("numbikesavailable") / F.col("capacity"))
    .withColumn("part_day", F.substring(F.col("duedate"), 1, 10))
    .withColumn("part_minute", F.substring(F.col("duedate"), 1, 16))
    .withColumn("lat", F.col("coordonnees_geo.lat"))
    .withColumn("lon", F.col("coordonnees_geo.lon"))
    .drop("code_insee_commune")
    .drop("nom_arrondissement")
    .drop("coordonnees_geo")
    .filter(F.col("part_day") >= "2024-01-01")
)

print("######################################")
print("3. Load into object storage")
print("######################################")

(
    df_transformed.write
    .partitionBy("part_day")
    .format("parquet")
    .mode("overwrite")
    .save(os.getenv('S3_OUTPUT_PATH'))
)

print("######################################")
print("4. Update hive metastore")
print("######################################")

host, port, user = 'trino-coordinator', 8080, 'trino'
conn = trino.dbapi.connect(host=host, port=port, user=user)
cur = conn.cursor()

catalog, schema, table = 'minio', 'velib_silver', 'velib_disponibilite_en_temps_reel'
schema_location = 's3a://velib/silver/'
partitioned_by = 'part_day'
external_location = 's3a://velib/silver/velib-disponibilite-en-temps-reel'
# List of queries to execute
queries = [
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} WITH (location = '{schema_location}')",
    f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}",
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
        capacity BIGINT,
        duedate VARCHAR,
        ebike BIGINT,
        is_installed VARCHAR,
        is_renting VARCHAR,
        is_returning VARCHAR,
        mechanical BIGINT,
        name VARCHAR,
        nom_arrondissement_communes VARCHAR,
        numbikesavailable BIGINT,
        numdocksavailable BIGINT,
        stationcode VARCHAR,
        fill_ratio DOUBLE,
        part_minute VARCHAR,
        lat DOUBLE,
        lon DOUBLE,
        part_day VARCHAR
    ) 
    WITH (
        format = 'PARQUET',
        partitioned_by = ARRAY['{partitioned_by}'],
        external_location = '{external_location}'
    )
    """,
    "USE minio.velib_silver",
    "CALL system.sync_partition_metadata('velib_silver', 'velib_disponibilite_en_temps_reel', 'ADD')",
    "SELECT * FROM minio.velib_silver.velib_disponibilite_en_temps_reel LIMIT 5"
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
        print(f"Error executing query: {query}. Error: {e}")

# Close the cursor and connection
cur.close()
conn.close()
