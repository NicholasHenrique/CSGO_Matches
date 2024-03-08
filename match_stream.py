# import findspark
# findspark.init()

import os
import delta
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType
from pyspark.sql import SparkSession, window
from pyspark.sql import functions as F

def upsertToDelta(df, batchId):
  '''
  In order to guarantee there aren't any duplicated matches, a Window is used to filter matches based on its GameId and UpdatedUtc.
  The GameId is used as a group by and UpdatedUtc is used as an order by.
  If it's found a duplicated match, the duplicate will be not be saved.
  '''
  windowSpec = window.Window.partitionBy("GameId").orderBy("UpdatedUtc")
  df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1")

  ( bronzeDeltaTable.alias("bronze")
                    .merge(df_new.alias("raw"), "bronze.GameId = raw.GameId")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
  )

builder = SparkSession.builder \
    .master('local[*]') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = delta.configure_spark_with_delta_pip(builder) \
        .getOrCreate()

match_schema = StructType([
    StructField("GameId", IntegerType(), False),
    StructField("RoundId", IntegerType(), False),
    StructField("Season", IntegerType(), False),
    StructField("SeasonType", IntegerType(), False),
    StructField("Group", StringType(), True),
    StructField("TeamAId", IntegerType(), True),
    StructField("TeamBId", IntegerType(), True),
    StructField("VenueId", IntegerType(), True),
    StructField("Day", StringType(), True),
    StructField("DateTime", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("Week", IntegerType(), True),
    StructField("BestOf", StringType(), True),
    StructField("Winner", StringType(), True),
    StructField("VenueType", StringType(), True),
    StructField("TeamAKey", StringType(), True),
    StructField("TeamAName", StringType(), True),
    StructField("TeamAScore", IntegerType(), True),
    StructField("TeamBKey", StringType(), True),
    StructField("TeamBName", StringType(), True),
    StructField("TeamBScore", IntegerType(), True),
    StructField("TeamAMoneyLine", IntegerType(), True),
    StructField("TeamBMoneyLine", IntegerType(), True),
    StructField("DrawMoneyLine", IntegerType(), True),
    StructField("PointSpread", DecimalType(), True),
    StructField("TeamAPointSpreadPayout", IntegerType(), True),
    StructField("TeamBPointSpreadPayout", IntegerType(), True),
    StructField("Updated", StringType(), True),
    StructField("UpdatedUtc", StringType(), True),
    StructField("IsClosed", BooleanType(), True)
])

TABLE_NAME = "csgo_match_history"

if "spark-warehouse" not in os.listdir():
  spark.sql("CREATE DATABASE bronze")

'''
Full load
'''
if TABLE_NAME not in os.listdir('spark-warehouse/bronze.db'):
  df = spark.read.schema(match_schema).format("json").load(f"raw/{TABLE_NAME}")
  '''
  In order to guarantee there aren't any duplicated matches, a Window is used to filter matches based on its GameId and UpdatedUtc.
  The GameId is used as a group by and UpdatedUtc is used as an order by.
  If it's found a duplicated match, the duplicate will be not be saved.
  '''
  windowSpec = window.Window.partitionBy("GameId").orderBy("UpdatedUtc")
  df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1").drop("row_number")
  df_new.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{TABLE_NAME}") # overwriting it's not overwrititng because it creates a different file name

bronzeDeltaTable = delta.tables.DeltaTable.forPath(spark, f"spark-warehouse/bronze.db/{TABLE_NAME}")

'''
When new matches lands in raw, a stream is responsible for saving these new matches in bronze.
'''
df_stream = ( spark.readStream
                   .format("json")
                   .schema(match_schema)
                   .load(f"raw/{TABLE_NAME}")
            )

stream = ( df_stream.writeStream
                    .foreachBatch(upsertToDelta)
                    .option("checkpointLocation", f"spark-warehouse/bronze.db/{TABLE_NAME}_checkpoint")
                    .outputMode("update")
                    .start()
         )

'''
Processing in batch with stream
'''
stream.processAllAvailable()
stream.stop()
spark.stop()