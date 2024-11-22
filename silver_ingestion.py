import utilities
import delta
import os
from pyspark.sql import SparkSession

def upsert(df, silverDeltaTable, on_fields):
    '''
    Upsert function to insert and update player's and matches' statistics.
    '''
    join = " and ".join([f"silver.{on_field} = bronze.{on_field}" for on_field in on_fields])
    ( silverDeltaTable.alias("silver")
                    .merge(df.alias("bronze"), join)
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
    )

warehouse_location = os.path.abspath('spark-warehouse')

builder = SparkSession.builder \
    .master('local[*]') \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .enableHiveSupport()

spark = delta.configure_spark_with_delta_pip(builder) \
        .getOrCreate()

table_on_fields = {
    "fs_players": ["PlayerId"], 
    "fs_teams": ["TeamName", "MapName"]
}

spark.sql("CREATE DATABASE IF NOT EXISTS silver")

for table, on_fields in table_on_fields.items():
    query = utilities.import_query(os.path.abspath(f'sql/{table}.sql'))
    df = spark.sql(query)
    if not utilities.table_exists('silver', table, spark):
        df.write.format("delta").mode("overwrite").saveAsTable(f'silver.{table}')
    else:
        silverDeltaTable = delta.tables.DeltaTable.forName(spark, f'silver.{table}')
        upsert(df, silverDeltaTable, on_fields)