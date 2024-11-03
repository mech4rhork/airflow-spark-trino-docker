import os
import sys
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import to_timestamp, concat, substring, lit
from pyspark.sql import DataFrame
import trino

    
class commonFunctions:
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
