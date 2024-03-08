# import findspark
# findspark.init()

import api_key
import requests
import os
import json
from delta import *
from pyspark.sql import SparkSession

builder = SparkSession.builder \
    .master('local[*]') \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .appName("CSGO_Matches")

spark = configure_spark_with_delta_pip(builder) \
        .getOrCreate()

def get_match_details(gameId):
  '''
  Collect match details from API.
  '''
  API_KEY = api_key.API_KEY
  url = f"https://api.sportsdata.io/v3/csgo/stats/json/BoxScore/{gameId}"
  response = requests.get(url, headers={"Ocp-Apim-Subscription-Key": f"{API_KEY}"})
  return response.json()

def get_and_land_match_details(gameId):
  '''
  Collect match details, save in raw/matches_landing a json file with its match details and save in raw/matches_proceeded a dataframe with the gameId and its path to where it's stored the match details in raw/matches_landing.
  '''
  data = get_match_details(gameId)
  if data:
    if "matches_landing" not in os.listdir("raw"): # talvez nao seja necessario
      os.mkdir("raw/matches_landing")
    path = f"raw/matches_landing/{gameId}.json"
    if path not in os.listdir("raw/matches_landing"):
      if "matches_proceeded" not in os.listdir("raw"): # talvez nao seja necessario
        os.mkdir("raw/matches_proceeded")
      with open(path, "w") as file:
        data_str = json.dumps(data)
        file.write(data_str)
      spark.createDataFrame([{"gameId": gameId, "path": path}]).write.format("parquet").mode("append").save("raw/matches_proceeded") #.format("delta")

def get_match_ids():
  '''
  Read matches in bronze table and compare with raw/matches_proceeded to return match ids that have not yet been detailed.
  '''
  df_history = spark.read.format("delta").load("spark-warehouse/bronze.db/csgo_match_history")
  try:
    df_proceeded = spark.read.parquet("raw/matches_proceeded") #.read.format("delta").load("raw/matches_proceeded")
    df_join = df_history.join(df_proceeded, "gameId", "left").filter("path is null").select("gameId")
    return df_join
  except:
    return df_history.select("gameId")

if __name__ == "__main__":
  '''
  Collect all match ids to get corresponding match details.
  '''
  game_ids = get_match_ids()
  for i in game_ids.collect():
    get_and_land_match_details(i[0])

spark.stop()