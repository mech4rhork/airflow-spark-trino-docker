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

spark = SparkSession.builder.getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

print("######################################")
print("1. Extract from object storage")
print("######################################")

input_path = f'{os.getenv("S3_INPUT_PATH")}/{os.getenv("PART_DAY")}/*'
df = spark.read.json(input_path)

print("######################################")
print("2. Transform")
print("######################################")

df_transformed = (
    df
    #
    .filter(F.col("is_installed") == "OUI")
    #
    .withColumn("fill_ratio", F.round(F.col("numbikesavailable") / F.col("capacity"), 3))
    .withColumn("fill_percentage", F.col("fill_ratio") * 100)
    #
    .withColumn("duedate_timestamp_minute", F.to_timestamp(
        F.concat(
            F.substring("duedate", 1, 16),      # Extracts the date and hour-minute part: "2024-10-29T01:07"
            F.lit(":00"),                       # Sets seconds explicitly to "00"
            F.substring("duedate", 20, 6)       # Extracts the timezone offset: "+00:00"
        ),
        "yyyy-MM-dd'T'HH:mm:ssXXX"
    ))
    .withColumn("polldate_timestamp_minute", 
        F.to_timestamp(
            F.split(F.split(F.input_file_name(), "/")[6], "\.")[0], 
            "yyyy-MM-dd-HHmm"
        )
    )
    .withColumn("polldate", F.concat(
        F.date_format(F.col("polldate_timestamp_minute"), "yyyy-MM-dd'T'HH:mm:ss"),
        F.lit("+00:00")
    ))
    #
    .withColumn("lat", F.col("coordonnees_geo.lat"))
    .withColumn("lon", F.col("coordonnees_geo.lon"))
    .drop("coordonnees_geo")
    #
    .withColumn("numero_departement", F.substring(F.col("code_insee_commune"), 0, 2))
    #
    .withColumn("part_month", F.substring(F.col("polldate"), 1, 7))
    .withColumn("part_day", F.substring(F.col("polldate"), 1, 10))
    .withColumn("part_minute", F.substring(F.col("polldate"), 1, 16))
    .filter(F.col("part_day") >= "2024-01-01")
)
df_transformed = df_transformed.select(*(sorted(df_transformed.columns)))

print("######################################")
print("3. Load into object storage")
print("######################################")

output_path = f'{os.getenv("S3_OUTPUT_PATH")}/part_day={os.getenv("PART_DAY")}' 
(
    df_transformed.write
    .format("parquet")
    .mode("overwrite")
    .save(output_path)
)

print("######################################")
print("4. Update hive metastore")
print("######################################")

host, port, user = 'trino-coordinator', 8080, 'trino'
conn = trino.dbapi.connect(host=host, port=port, user=user)
cur = conn.cursor()

catalog, schema, table = 'minio', f'{os.getenv("TRINO_OUTPUT_SCHEMA")}', f'{os.getenv("TRINO_OUTPUT_TABLE")}'
schema_location = f'{os.getenv("TRINO_OUTPUT_SCHEMA_LOCATION")}'
partitioned_by = 'part_day'
external_location = f'{os.getenv("TRINO_OUTPUT_TABLE_LOCATION")}'

# List of queries to execute
queries = [
    f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} WITH (location = '{schema_location}')",
    f"DROP TABLE IF EXISTS {catalog}.{schema}.{table}",
    
    sparkDatalakeUtils.commonFunctions.generate_trino_create_table(df_transformed, catalog, schema, table, partitioned_by, external_location),
    
    f"USE {catalog}.{schema}",
    f"CALL system.sync_partition_metadata('{schema}', '{table}', 'ADD')",
    f"SELECT * FROM {catalog}.{schema}.{table} WHERE part_day = '" + os.getenv("PART_DAY") + "' LIMIT 10"
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
