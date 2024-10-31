import os
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType
import trino

# Variables d'environnement
os.environ['PYSPARK_SUBMIT_ARGS'] = (
    '--packages org.apache.hadoop:hadoop-aws:3.2.0,org.apache.hadoop:hadoop-common:3.2.0,io.trino:trino-jdbc:422 '
    'pyspark-shell'
)
os.environ['S3_ENDPOINT'] = "http://minio:9000"
os.environ['AWS_ACCESS_KEY_ID'] = "minio"
os.environ['AWS_SECRET_ACCESS_KEY'] = "minio123"

# On reprend la config S3 de l'autre fichier
spark = (
    SparkSession.builder
    # .master("spark://spark:7077")
    .appName("spark-silver-to-gold-1hour")
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", os.getenv("S3_ENDPOINT"))
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.attempts.maximum", "1")
    .config("spark.hadoop.fs.s3a.connection.establish.timeout", "5000")
    .config("spark.hadoop.fs.s3a.connection.timeout", "10000")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

#Comme c'est bien foutu on peut juste lire la table en parquet comme on lisait le json
silver_path = "s3a://velib/silver/velib-disponibilite-en-temps-reel"
df_silver = spark.read.parquet(silver_path)

# Turnover (mouvements) par heure par station
turnover_window = Window.partitionBy("stationcode").orderBy("duedate")
df_gold_turnover = (
    df_silver
    .withColumn("part_minute_dt", F.to_timestamp("duedate"))  # Convert string to timestamp
    .withColumn("turnover", F.abs(F.lag("numbikesavailable", 1).over(turnover_window) - F.col("numbikesavailable")))
    .withColumn("hour", F.date_trunc("hour", "part_minute_dt"))  # Truncate to hour
    .groupBy("stationcode", "name", "hour", "lat", "lon")  # Include 'name' in groupBy
    .agg(
        F.sum("turnover").alias("total_turnover"),
    )
)
    

# Stats par heure sur l'ensemble des stations
df_gold_stats = (
    df_silver
    .filter(~(F.col("name").isNull() | (F.lower(F.col("name")) == "None")))
    .filter(
        ~(
            F.col("name").isNull() | 
            (F.lower(F.col("name")) == "none") | 
            (F.to_date(F.substring(F.col("duedate"), 1, 10), "yyyy-MM-dd") < "2024-10-29")
        )
    )
    .withColumn("part_minute_dt", F.to_timestamp("duedate"))
    .withColumn("hour", F.date_trunc("hour", "part_minute_dt"))
    .groupBy("hour")
    .agg(
        F.mean("numbikesavailable").alias("avg_bikes"),
        F.expr("percentile_approx(numbikesavailable, 0.5)").cast(IntegerType()).alias("median_bikes"),
        F.min("numbikesavailable").alias("min_bikes"),
        F.max("numbikesavailable").alias("max_bikes"),
        F.mean("fill_ratio").alias("avg_fill_ratio"),
        F.expr("percentile_approx(fill_ratio, 0.5)").cast(IntegerType()).alias("median_fill_ratio"),
        F.min("fill_ratio").alias("min_fill_ratio"),
        F.max("fill_ratio").alias("max_fill_ratio")
    )
    .orderBy( "hour")
)

# Stats par heure pour chacune des stations
df_gold_station_stats = (
    df_silver
    .filter(~(F.col("name").isNull() | (F.lower(F.col("name")) == "None")))
    .filter(
        ~(
            F.col("name").isNull() | 
            (F.lower(F.col("name")) == "none") | 
            (F.to_date(F.substring(F.col("duedate"), 1, 10), "yyyy-MM-dd") < "2024-10-29")
        )
    )
    .withColumn("part_minute_dt", F.to_timestamp("duedate"))
    .withColumn("hour", F.date_trunc("hour", "part_minute_dt"))
    .groupBy("stationcode", "name", "hour", "lat", "lon")
    .agg(
        F.mean("numbikesavailable").alias("avg_bikes"),
        F.expr("percentile_approx(numbikesavailable, 0.5)").cast(IntegerType()).alias("median_bikes"),
        F.min("numbikesavailable").alias("min_bikes"),
        F.max("numbikesavailable").alias("max_bikes"),
        F.mean("fill_ratio").alias("avg_fill_ratio"),
        F.expr("percentile_approx(fill_ratio, 0.5)").cast(IntegerType()).alias("median_fill_ratio"),
        F.min("fill_ratio").alias("min_fill_ratio"),
        F.max("fill_ratio").alias("max_fill_ratio")
    )
    .orderBy("stationcode", "hour")
)

host, port, user = 'trino-coordinator', 8080, 'trino'
conn = trino.dbapi.connect(host=host, port=port, user=user)
cur = conn.cursor()
def createTable(df, catalog, schema_name, table, schema_location, schema, partitioned_by, external_location):
    os.environ['S3_OUTPUT_PATH'] = external_location
    #spark.sparkContext.setLogLevel("WARN")
    # Write DataFrame to S3
    (
        df.write
        .partitionBy(partitioned_by)
        .format("parquet")
        .mode("overwrite")
        .save(os.getenv('S3_OUTPUT_PATH'))
    )
    
    
    host, port, user = 'trino-coordinator', 8080, 'trino'
    conn = trino.dbapi.connect(host=host, port=port, user=user)
    cur = conn.cursor()
    
    queries = [
        f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema_name} WITH (location = '{schema_location}')",
        f"DROP TABLE IF EXISTS {catalog}.{schema_name}.{table}",
        f"""
        CREATE TABLE IF NOT EXISTS {catalog}.{schema_name}.{table} (
        {schema}
        )
        WITH (
            format = 'PARQUET',
            partitioned_by = ARRAY['{partitioned_by}'],
            external_location = '{external_location}'
        )
        """,
        f"USE {catalog}.{schema_name}",
        f"CALL system.sync_partition_metadata('{schema_name}', '{table}', 'ADD')",
        # f"CALL system.sync_partition_metadata('{catalog}.{schema_name}.{table}', 'ADD')",
        f"SELECT * FROM {catalog}.{schema_name}.{table} LIMIT 5"
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


schema_turnover = f"""
    name VARCHAR,
    hour TIMESTAMP,
    lat DOUBLE,
    lon DOUBLE,
    total_turnover BIGINT,
    stationcode VARCHAR
"""

createTable(df_gold_turnover, 'minio', 'velib_gold', 'velib_turnover', 's3a://velib/gold/', schema_turnover, 'stationcode', 's3a://velib/gold/velib_turnover')
 

schema_stats_all = f"""
    avg_bikes DOUBLE,
    median_bikes INTEGER,
    min_bikes BIGINT,
    max_bikes BIGINT,
    avg_fill_ratio DOUBLE,
    median_fill_ratio INTEGER,
    min_fill_ratio DOUBLE,
    max_fill_ratio DOUBLE,
    hour TIMESTAMP
"""

createTable(df_gold_stats, 'minio', 'velib_gold', 'velib_stats_all', 's3a://velib/gold/', schema_stats_all, 'hour', 's3a://velib/gold/velib_stats_all')

schema_stats_station = f"""
    name VARCHAR,
    hour TIMESTAMP,
    lat DOUBLE,
    lon DOUBLE,
    avg_bikes DOUBLE,
    median_bikes INTEGER,
    min_bikes BIGINT,
    max_bikes BIGINT,
    avg_fill_ratio DOUBLE,
    median_fill_ratio INTEGER,
    min_fill_ratio DOUBLE,
    max_fill_ratio DOUBLE,
    stationcode VARCHAR
"""

createTable(df_gold_station_stats, 'minio', 'velib_gold', 'velib_stats_station', 's3a://velib/gold/', schema_stats_station, 'stationcode', 's3a://velib/gold/velib_stats_station')