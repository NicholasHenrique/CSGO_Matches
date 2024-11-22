import delta
import os
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import to_timestamp, regexp_replace
from functools import partial

def upsertToDelta(df, batchId, bronzeDeltaTable, table_name, on_fields):
    '''
    Upsert function to insert and update matches details. It also transforms tb_leaderboards' timestamp columns into valid formats.
    '''
    if table_name == "tb_leaderboards":
        df = df.withColumn("Day", to_timestamp(regexp_replace("Day", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                .withColumn("DateTime", to_timestamp(regexp_replace("DateTime", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                .withColumn("Updated", to_timestamp(regexp_replace("Updated", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                .withColumn("UpdatedUtc", to_timestamp(regexp_replace("UpdatedUtc", 'T', ' '), 'yyyy-MM-dd HH:mm:ss'))

    join_condition = " and ".join([f"bronze.{field} = raw.{field}" for field in on_fields])

    ( bronzeDeltaTable.alias("bronze")
                      .merge(df.alias("raw"), join_condition)
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

table_on_fields = {
    "tb_leaderboards": ["PlayerId", "GameId", "MapName"], 
    "tb_maps": ["Number", "GameId"]
}

for TABLE_NAME in list(SCHEMAS.keys()):
    '''
    Full load
    '''
    if TABLE_NAME not in os.listdir('spark-warehouse/bronze.db'):
        df = spark.read.schema(SCHEMAS[TABLE_NAME]).parquet(f"raw/{TABLE_NAME}")
        '''
        Transformation of tb_leaderboards' timestamp columns into valid formats.
        '''
        if TABLE_NAME == "tb_leaderboards":
            df = df.withColumn("Day", to_timestamp(regexp_replace("Day", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                        .withColumn("DateTime", to_timestamp(regexp_replace("DateTime", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                        .withColumn("Updated", to_timestamp(regexp_replace("Updated", 'T', ' '), 'yyyy-MM-dd HH:mm:ss')) \
                        .withColumn("UpdatedUtc", to_timestamp(regexp_replace("UpdatedUtc", 'T', ' '), 'yyyy-MM-dd HH:mm:ss'))
        df.write.mode("overwrite").format("delta").saveAsTable(f"bronze.{TABLE_NAME}")
    else:
        bronzeDeltaTable = delta.tables.DeltaTable.forName(spark, f"bronze.{TABLE_NAME}")

        '''
        When new matches land in raw, a stream is responsible for saving these new matches in bronze.
        '''
        df_stream = ( spark.readStream
                            .format("parquet")
                            .schema(SCHEMAS[TABLE_NAME])
                            .load(f"raw/{TABLE_NAME}")
                    )

        upsertToDeltaFunc = partial(upsertToDelta, bronzeDeltaTable=bronzeDeltaTable, table_name=TABLE_NAME, on_fields=table_on_fields[TABLE_NAME])

        stream = ( df_stream.writeStream
                            .foreachBatch(upsertToDeltaFunc)
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