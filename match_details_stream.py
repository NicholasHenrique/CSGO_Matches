import findspark
findspark.init()

import delta
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import SparkSession, window
from pyspark.sql import functions as F
from os.path import abspath

def upsertToDelta(df, batchId):
  '''
  In order to guarantee there aren't any duplicated matches, a Window is used to filter matches based on its GameId and UpdatedUtc.
  The GameId is used as a group by and UpdatedUtc is used as an order by.
  If it's found a duplicated match, the duplicate will be not be saved.
  '''
  windowSpec = window.Window.partitionBy("GameId").orderBy("UpdatedUtc") # .orderBy(1)
  df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1")

  ( bronzeDeltaTable.alias("bronze")
                    .merge(df_new.alias("raw"), "bronze.GameId = raw.GameId")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
  )

warehouse_location = abspath('spark-warehouse')

builder = SparkSession.builder \
    .master('local[*]') \
    .config("spark.sql.warehouse.dir", warehouse_location) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = delta.configure_spark_with_delta_pip(builder) \
        .getOrCreate()

leaderboards_schema = StructType([
    StructField("PlayerId", IntegerType(), False),
    StructField("TeamId", IntegerType(), False),
    StructField("Name", StringType(), False),
    StructField("MatchName", StringType(), False),
    StructField("Team", StringType(), False),
    StructField("IsClosed", BooleanType(), False),
    StructField("GameId", IntegerType(), False),
    StructField("OpponentId", IntegerType(), False),
    StructField("Opponent", StringType(), False),
    StructField("Day", StringType(), True),
    StructField("DateTime", StringType(), True),
    StructField("Updated", StringType(), True),
    StructField("UpdatedUtc", StringType(), True),
    StructField("Games", StringType(), True),
    StructField("Maps", FloatType(), True),
    StructField("FantasyPoints", FloatType(), True),
    StructField("Kills", FloatType(), True),
    StructField("Assists", FloatType(), True),
    StructField("Deaths", FloatType(), True),
    StructField("Headshots", FloatType(), True),
    StructField("AverageDamagePerRound", FloatType(), True),
    StructField("Kast", FloatType(), True),
    StructField("Rating", FloatType(), True),
    StructField("EntryKills", FloatType(), True),
    StructField("QuadKills", FloatType(), True),
    StructField("Aces", FloatType(), True),
    StructField("Clutch1v2s", FloatType(), True),
    StructField("Clutch1v3s", FloatType(), True),
    StructField("Clutch1v4s", FloatType(), True),
    StructField("Clutch1v5s", FloatType(), True),
])

map_schema = StructType([
    StructField("Number", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("CurrentRound", IntegerType(), True),
    StructField("TeamAScore", IntegerType(), True),
    StructField("TeamBScore", IntegerType(), True),
])

SCHEMAS = {
  "tb_leaderboards": leaderboards_schema,
  "tb_maps": map_schema
}

if "spark-warehouse" not in os.listdir():
  spark.sql("CREATE DATABASE bronze")

try:
  for TABLE_NAME in list(SCHEMAS.keys()):
    '''
    Full load
    '''
    if TABLE_NAME not in os.listdir('spark-warehouse/bronze.db'):
      df = spark.read.parquet(f"raw/{TABLE_NAME}")
      windowSpec = window.Window.partitionBy("GameId").orderBy("UpdatedUtc") # .orderBy(1)
      df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1").drop("row_number")
      df_new.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{TABLE_NAME}") # overwriting it's not overwrititng because it creates a different file name
      # df_new.write.format("delta").saveAsTable(name=f"{warehouse_location}.bronze.{TABLE_NAME}", mode="overwrite")
      # df_new.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{TABLE_NAME}")

    bronzeDeltaTable = delta.tables.DeltaTable.forPath(spark, f"spark-warehouse/bronze.db/{TABLE_NAME}") #"bronze"

    '''
    When new matches lands in raw, a stream is responsible for saving these new matches in bronze.
    '''
    df_stream = ( spark.readStream
                      .format("parquet")
                      .schema(SCHEMAS[TABLE_NAME])
                      .load(f"raw/{TABLE_NAME}")
                )

    stream = ( df_stream.writeStream
                        .foreachBatch(upsertToDelta)
                        .option("checkpointLocation", f"spark-warehouse/bronze.db/{TABLE_NAME}_checkpoint")
                        .outputMode("update")
                        .start()
            )

    stream.processAllAvailable()
    stream.stop()
finally:
  spark.stop()