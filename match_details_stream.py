"""
como o match_stream.py salva na bronze partidas únicas, então de acordo com o funcionamento do get_match_details.py,
não haverá partidas duplicadas em raw/matches_proceeded e, portanto, não haverá partidas duplicadas em raw/tb_maps e raw/tb_leaderboards,
logo não há a necessidade de criar uma window para que só pegue as partidas únicas em raw/tb_maps e raw/tb_leaderboards
entretanto quando tira a window, dá o erro:
pyspark.errors.exceptions.captured.UnsupportedOperationException: [DELTA_MULTIPLE_SOURCE_ROW_MATCHING_TARGET_ROW_IN_MERGE] Cannot perform Merge as multiple source rows matched and attempted to modify the same
target row in the Delta table in possibly conflicting ways. By SQL semantics of Merge,
when multiple source rows match on the same target row, the result may be ambiguous
as it is unclear which source row should be used to update or delete the matching
target row. You can preprocess the source table to eliminate the possibility of
multiple matches. Please refer to
https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge
"""

import delta
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import SparkSession, window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_timestamp, regexp_replace

def upsertToDelta(df, batchId):
  '''
  In order to guarantee there aren't any duplicated matches, a Window is used to filter matches based on its GameId and UpdatedUtc.
  The GameId is used as a group by and UpdatedUtc is used as an order by.
  If it's found a duplicated match, the duplicate will be not be saved.
  '''
  windowSpec = window.Window.partitionBy("GameId").orderBy(col("UpdatedUtc").desc()) #.orderBy(F.lit(1)) # .orderBy(F.lit(1)) .orderBy("UpdatedUtc")
  df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1")
  df_new = df_new.withColumn("Day", to_timestamp(regexp_replace("Day", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                   .withColumn("DateTime", to_timestamp(regexp_replace("DateTime", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                   .withColumn("Updated", to_timestamp(regexp_replace("Updated", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                   .withColumn("UpdatedUtc", to_timestamp(regexp_replace("UpdatedUtc", 'T', ' '), 'yyyy-MM-dd HH:mm:ss'))

  ( bronzeDeltaTable.alias("bronze")
                    .merge(df_new.alias("raw"), "bronze.GameId = raw.GameId")
                    # .merge(df.alias("raw"), "bronze.GameId = raw.GameId")
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
    StructField("MapName", StringType(), False)
])

map_schema = StructType([
    StructField("Number", IntegerType(), True),
    StructField("Name", StringType(), True),
    StructField("Status", StringType(), True),
    StructField("CurrentRound", IntegerType(), True),
    StructField("TeamAScore", IntegerType(), True),
    StructField("TeamBScore", IntegerType(), True),
    StructField("GameId", IntegerType(), False),
    StructField("TeamAName", StringType(), True),
    StructField("TeamBName", StringType(), True),
    StructField("TeamAKey", StringType(), True),
    StructField("TeamBKey", StringType(), True)
])

SCHEMAS = {
  "tb_leaderboards": leaderboards_schema,
  "tb_maps": map_schema
}

spark.sql("CREATE DATABASE IF NOT EXISTS bronze")

try:
  for TABLE_NAME in list(SCHEMAS.keys()):
    '''
    Full load
    '''
    if TABLE_NAME not in os.listdir('spark-warehouse/bronze.db'):
      df = spark.read.parquet(f"raw/{TABLE_NAME}")
      windowSpec = window.Window.partitionBy("GameId").orderBy(col("UpdatedUtc").desc()) #.orderBy(F.lit(1)) # .orderBy(F.lit(1)) .orderBy("UpdatedUtc")
      df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1").drop("row_number")

      # df.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{TABLE_NAME}")
      df_new = df_new.withColumn("Day", to_timestamp(regexp_replace("Day", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                   .withColumn("DateTime", to_timestamp(regexp_replace("DateTime", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                   .withColumn("Updated", to_timestamp(regexp_replace("Updated", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                   .withColumn("UpdatedUtc", to_timestamp(regexp_replace("UpdatedUtc", 'T', ' '), 'yyyy-MM-dd HH:mm:ss'))
      df_new.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{TABLE_NAME}")
    else:
      bronzeDeltaTable = delta.tables.DeltaTable.forName(spark, f"bronze.{TABLE_NAME}")

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