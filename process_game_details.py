from pyspark.sql.types import StructType, StructField, IntegerType, StringType, BooleanType, FloatType
from pyspark.sql import functions as F
import json

import findspark
findspark.init()

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSGO_Matches") \
    .master('local[*]') \
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

# map_name = list(match[0].items())[1][1][0]['Name']
# leaderboards = [dict(item, **{'MapName':f'{map_name}'}) for item in list(match[0].items())[1][1][0]['Leaderboards']]

# df = spark.createDataFrame(leaderboards, schema=leaderboards_schema)
# df.show()

# o lógico seria estar vindo mais de um mapa quando fosse Bo3, mas sempre vem 1

def get_data(gameId):
  path = f"raw/games_landing/{gameId}.json"
  return json.load(open(path, 'r'))

def save_df(df, table):
  (
    df.coalesce(1)
      .write
      .mode("append")
      .format("parquet")
      .save(f"raw/{table}")
  )

def create_leaderboards_map_df(data):
  maps = list(data[0].items())[1][1]
  leaderboards = maps[0]['Leaderboards']
  map_name = maps[0]['Name']
  df_leaderboards = spark.createDataFrame(leaderboards, schema=leaderboards_schema)
  df_leaderboards = df_leaderboards.withColumn("MapName", F.lit(map_name))
  save_df(df_leaderboards, "tb_leaderboards")

def create_maps_df(data):
  maps = list(data[0].items())[1][1]
  game = list(data[0].items())[0][1]
  gameId = game['GameId']
  teamAName = game['TeamAName']
  teamBName = game['TeamBName']
  teamAKey = game['TeamAKey']
  teamBKey = game['TeamBKey']
  df_maps = spark.createDataFrame(maps, schema=map_schema)
  df_maps = df_maps.withColumn("GameId", F.lit(gameId)).withColumn("TeamAName", F.lit(teamAName)).withColumn("TeamBName", F.lit(teamBName)).withColumn("TeamAKey", F.lit(teamAKey)).withColumn("TeamBKey", F.lit(teamBKey))
  save_df(df_maps, "tb_maps")

def process_game(gameId):
  data = get_data(gameId)
  create_leaderboards_map_df(data)
  create_maps_df(data)

def get_ids():
  df_proceeded = spark.read.parquet("raw/games_proceeded") #.read.format("delta").load("raw/games_proceeded")
  try:
    df_maps = spark.read.parquet("raw/tb_maps")
    df_join = df_proceeded.join(df_maps, "gameId", "left").filter("Status is null")
    return df_join.select("gameId")
  except:
    return df_proceeded.select("gameId")

game_ids = get_ids()
for i in game_ids.collect():
    process_game(i[0])

spark.stop()