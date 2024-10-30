import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp, concat, substring, lit
from pyspark.sql import DataFrame
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
    .withColumn("part_month", F.substring(F.col("polldate"), 1, 7))
    .withColumn("part_day", F.substring(F.col("polldate"), 1, 10))
    .withColumn("part_minute", F.substring(F.col("polldate"), 1, 16))
    .filter(F.col("part_day") >= "2024-01-01")
)
df_transformed = df_transformed.select(*(sorted(df_transformed.columns)))

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

def generate_trino_create_table(
    df: DataFrame, 
    catalog: str, 
    schema: str, 
    table: str, 
    partitioned_by: str, 
    external_location: str
) -> str:
    # Comprehensive mapping of Spark SQL types to Trino types
    type_mapping = {
        "bigint": "BIGINT",
        "binary": "VARBINARY",
        "boolean": "BOOLEAN",
        "decimal": "DECIMAL",  # Precision and scale will need to be handled separately if defined
        "double": "DOUBLE",
        "float": "REAL",
        "int": "INTEGER",
        "smallint": "SMALLINT",
        "string": "VARCHAR",
        "timestamp": "TIMESTAMP",
        "tinyint": "TINYINT"
    }
    
    # Separate columns to ensure partition column is placed last
    columns = []
    partition_column = None
    for field in df.schema.fields:
        spark_type = field.dataType.simpleString()
        trino_type = type_mapping.get(spark_type, "VARCHAR")  # default to VARCHAR if no match
        # DEBUG # print(f'field.name={field.name}, spark_type={spark_type}, trino_type={trino_type}')
        if field.name == partitioned_by:
            partition_column = f"{field.name} {trino_type}"
        else:
            columns.append(f"{field.name} {trino_type}")
    
    # Add the partition column to the end if it exists in the schema
    if partition_column:
        columns.append(partition_column)
    
    # Join column definitions into a single string
    columns_definition = ",\n    ".join(columns)
    
    # Generate the final CREATE TABLE statement
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table} (
        {columns_definition}
    ) 
    WITH (
        format = 'PARQUET',
        partitioned_by = ARRAY['{partitioned_by}'],
        external_location = '{external_location}'
    )
    """
    
    return create_table_sql.strip()

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
    
    generate_trino_create_table(df_transformed, catalog, schema, table, partitioned_by, external_location),
    
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
        print(f"Error executing query: {query}. Error: {e}")

# Close the cursor and connection
cur.close()
conn.close()
