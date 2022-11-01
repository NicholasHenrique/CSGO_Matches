from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, FloatType
from pyspark.sql import SparkSession, window
from pyspark.sql import functions as F
import delta
import os

builder = SparkSession.builder \
    .master('local[*]') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = delta.configure_spark_with_delta_pip(builder) \
        .getOrCreate()

# schema = StructType([
#     StructField("GameId", IntegerType(), False),
#     StructField("RoundId", IntegerType(), False),
#     StructField("Season", IntegerType(), False),
#     StructField("SeasonType", IntegerType(), False),
#     StructField("Group", StringType(), True),
#     StructField("TeamAId", IntegerType(), True),
#     StructField("TeamBId", IntegerType(), True),
#     StructField("VenueId", IntegerType(), True),
#     StructField("Day", StringType(), True),
#     StructField("DateTime", StringType(), True),
#     StructField("Status", StringType(), True),
#     StructField("Week", IntegerType(), True),
#     StructField("BestOf", StringType(), True),
#     StructField("Winner", StringType(), True),
#     StructField("VenueType", StringType(), True),
#     StructField("TeamAKey", StringType(), True),
#     StructField("TeamAName", StringType(), True),
#     StructField("TeamAScore", IntegerType(), True),
#     StructField("TeamBKey", StringType(), True),
#     StructField("TeamBName", StringType(), True),
#     StructField("TeamBScore", IntegerType(), True),
#     StructField("TeamAMoneyLine", IntegerType(), True),
#     StructField("TeamBMoneyLine", IntegerType(), True),
#     StructField("DrawMoneyLine", IntegerType(), True),
#     StructField("PointSpread", DecimalType(), True),
#     StructField("TeamAPointSpreadPayout", IntegerType(), True),
#     StructField("TeamBPointSpreadPayout", IntegerType(), True),
#     StructField("Updated", StringType(), True),
#     StructField("UpdatedUtc", StringType(), True),
#     StructField("IsClosed", BooleanType(), True)
# ])

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

TABLE_NAME = "tb_leaderboards"

schema = spark.read.parquet(f"raw/{TABLE_NAME}").schema

if "spark-warehouse" not in os.listdir():
  spark.sql("CREATE DATABASE bronze")

if TABLE_NAME not in os.listdir('spark-warehouse/bronze.db'):
  df = spark.read.parquet(f"raw/{TABLE_NAME}")
  windowSpec = window.Window.partitionBy("GameId").orderBy("UpdatedUtc") # .orderBy(1)
  df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1").drop("row_number")
  # spark.sql("CREATE DATABASE bronze")
  df_new.write.mode("overwrite").format("delta").saveAsTable(f"`spark-warehouse`.bronze.{TABLE_NAME}") # overwrite não está sobreescrevendo pois quando o comando é executado ele atribui um novo nome para o arquivo (?)
  # df_new.distinct().count()

  # na primeira execução, ele não está lendo o arquivo existente na bronze, então ele adiciona todos os dados novos únicos

bronzeDeltaTable = delta.tables.DeltaTable.forPath(spark, f"spark-warehouse/bronze.db/{TABLE_NAME}")

def upsertToDelta(df, batchId):
  windowSpec = window.Window.partitionBy("GameId").orderBy("UpdatedUtc") # .orderBy(1)
  df_new = df.withColumn("row_number", F.row_number().over(windowSpec)).filter("row_number = 1")

  ( bronzeDeltaTable.alias("bronze")
                    .merge(df_new.alias("raw"), "bronze.GameId = raw.GameId")
                    .whenMatchedUpdateAll()
                    .whenNotMatchedInsertAll()
                    .execute()
  )

# df_stream = ( spark.readStream
#                    .format("cloudFiles")
#                    .option("cloudFiles.format", "json")
#                    .schema(schema)
#                    .load("raw/csgo_match_history")
#             )

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