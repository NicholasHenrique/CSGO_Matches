import requests
import os
import json
from delta import *

import findspark
findspark.init()

from pyspark.sql import SparkSession

builder = SparkSession.builder \
    .master('local[*]') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .appName("CSGO_Matches")

spark = configure_spark_with_delta_pip(builder) \
        .getOrCreate()

def get_data(gameId):
  api_key = "dedaad020f8d43b2aafcb7d1460a1f4b"
  url = f"https://api.sportsdata.io/v3/csgo/stats/json/BoxScore/{gameId}"
  response = requests.get(url, headers={"Ocp-Apim-Subscription-Key": f"{api_key}"})
  return response.json()

def get_and_land_game_details(gameId):
  data = get_data(gameId)
  if data:
    if "games_landing" not in os.listdir("raw"): # talvez nao seja necessario
      os.mkdir("raw/games_landing")
    path = f"raw/games_landing/{gameId}.json"
    if path not in os.listdir("raw/games_landing"):
      if "games_proceeded" not in os.listdir("raw"): # talvez nao seja necessario
        os.mkdir("raw/games_proceeded")
      with open(path, "w") as file:
        data_str = json.dumps(data)
        file.write(data_str)
      spark.createDataFrame([{"gameId": gameId, "path": path}]).write.format("parquet").mode("append").save("raw/games_proceeded") #.format("delta")

def get_games_ids():
  df_history = spark.read.format("delta").load("spark-warehouse/bronze.db/csgo_match_history")
  try:
    df_proceeded = spark.read.parquet("raw/games_proceeded") #.read.format("delta").load("raw/games_proceeded")
    df_join = df_history.join(df_proceeded, "gameId", "left").filter("path is null").select("gameId")
    return df_join
  except:
    return df_history.select("gameId")

if __name__ == "__main__":
  game_ids = get_games_ids()
  for i in game_ids.collect():
    get_and_land_game_details(i[0])

spark.stop()