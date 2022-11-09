import findspark
findspark.init()

from pyspark.sql import SparkSession

import requests
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType
from datetime import datetime, timedelta, date
import sys
import os

import api_key

spark = SparkSession.builder \
    .appName("CSGO_Matches") \
    .master('local[*]') \
    .getOrCreate()

schema = StructType([
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

def get_data(date):
  API_KEY = api_key.API_KEY
  url = f"https://api.sportsdata.io/v3/csgo/scores/json/GamesByDate/{date}"
  response = requests.get(url, headers={"Ocp-Apim-Subscription-Key": f"{API_KEY}"})
  return response.json()

def get_min_match_date(df):
  # df.groupBy().agg(F.min("Day")).collect()[0][0].split('T')[0]
  return df.agg(F.min("Day")).collect()[0][0].split('T')[0]

def get_max_match_date(df):
  # df.groupBy().agg(F.min("Day")).collect()[0][0].split('T')[0]
  return df.agg(F.max("Day")).collect()[0][0].split('T')[0]

def save_match_list(df):
  # if "raw" not in os.listdir():
  #   os.mkdir("raw")
  df.coalesce(1).write.format("json").mode("append").save("raw/csgo_match_history")

def get_history_matches(**kwargs):
  df = spark.read.format("json").load("raw/csgo_match_history")
  date = get_min_match_date(df)
  i = 0
  while i <= 6: # coleta 7 dias de partidas anteriores
    date = (datetime.strptime(date,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    data = get_data(date=date)
    # print(i)
    i += 1
    if not data:
      i -= 1
      continue
    df_data = spark.createDataFrame(data, schema=schema)
    # print(date)
    save_match_list(df_data)

def get_new_matches(**kwargs):
  df = spark.read.format("json").load("raw/csgo_match_history")
  max_date = get_max_match_date(df)
  today = date.today().strftime('%Y-%m-%d')
  while max_date <= today:
    max_date = (datetime.strptime(max_date,'%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    data = get_data(date=max_date)
    df_data = spark.createDataFrame(data, schema=schema)
    save_match_list(df_data)

def first_load(date):
  '''
  Função para fazer o carregamento de dados pela primeira vez.
  Caso não haja jogos no dia passado como argumento, tenta-se pegar dados do dia anterior.
  Isto deve ser feito, pois, se não, a função get_history_matches não receberá nenhum dado da função get_min_match_date que tentará retornar o menor valor para o campo "Day" do dataset.
  '''
  data = get_data(date=date)
  while not data:
    date = (datetime.strptime(date,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    data = get_data(date=date)
  df_data = spark.createDataFrame(data, schema=schema)
  save_match_list(df_data)

if __name__ == '__main__':
  match sys.argv[1]:
    case "new":
      if "raw/csgo_match_history" not in os.listdir():
        today = date.today().strftime('%Y-%m-%d')
        first_load(today)
      else:
        get_new_matches()
    case "history":
      if "raw/csgo_match_history" not in os.listdir():
        today = date.today().strftime('%Y-%m-%d')
        first_load(today)
      get_history_matches()
    case _:
      print("invalid")

spark.stop()