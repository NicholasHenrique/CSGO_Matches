import findspark
findspark.init()

import api_key
import requests
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType
from datetime import datetime, timedelta, date

spark = SparkSession.builder \
    .appName("CSGO_Matches") \
    .master('local[*]') \
    .getOrCreate()

'''
Json structure of match returned from API.
'''
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

def get_match(date):
  '''
  Collects a match from API based on date argument.
  '''
  API_KEY = api_key.API_KEY
  url = f"https://api.sportsdata.io/v3/csgo/scores/json/GamesByDate/{date}"
  response = requests.get(url, headers={"Ocp-Apim-Subscription-Key": f"{API_KEY}"})
  return response.json()

def get_min_match_date(df):
  '''
  Returs the oldest day of matches stored in raw.
  '''
  # df.groupBy().agg(F.min("Day")).collect()[0][0].split('T')[0]
  return df.agg(F.min("Day")).collect()[0][0].split('T')[0]

def get_max_match_date(df):
  '''
  Returns the most recent day of matches stored in raw.
  '''
  return df.agg(F.max("Day")).collect()[0][0].split('T')[0]

def save_match_list(df):
  '''
  Saves in raw the match in json format sent by API.
  '''
  df.coalesce(1).write.format("json").mode("append").save("raw/csgo_match_history")

def get_history_matches():
  '''
  Function to collect older matches based on the oldest match stored in raw.
  The matches are read from raw and the oldest date of thoses matches is identified to collect matches from the last 7 days based on the identified date.
  '''
  df = spark.read.format("json").load("raw/csgo_match_history")
  date = get_min_match_date(df)
  i = 0
  while i <= 6:
    date = (datetime.strptime(date,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    match = get_match(date=date)
    i += 1
    if not match:
      i -= 1
      continue
    df_match = spark.createDataFrame(match, schema=schema)
    save_match_list(df_match)

def get_new_matches():
  '''
  Function to collect new matches based on the most recent match stored in raw.
  The matches are read from raw and the most recent date of those matches is identified to collect matches up to the code execution day.
  '''
  df = spark.read.format("json").load("raw/csgo_match_history")
  max_date = get_max_match_date(df)
  today = date.today().strftime('%Y-%m-%d')
  while max_date <= today:
    max_date = (datetime.strptime(max_date,'%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')
    match = get_match(date=max_date)
    df_match = spark.createDataFrame(match, schema=schema)
    save_match_list(df_match)

def first_load(date):
  '''
  Function to load matches for the first time.
  In case there are no matches on the day passed as argument, it'll get matches on the day before and so on.
  This must be done so get_history_matches or get_new_matches functions can call get_min_match_date and get_max_match_date, respectively, without any error.
  '''
  match = get_match(date=date)
  while not match:
    date = (datetime.strptime(date,'%Y-%m-%d') - timedelta(days=1)).strftime('%Y-%m-%d')
    match = get_match(date=date)
  df_match = spark.createDataFrame(match, schema=schema)
  save_match_list(df_match)

if __name__ == '__main__':
  '''
  When this function is directly called, a parameter with two options ("new" or "history") determines the code execution.

  If "new" option is passed, the code will collect new matches from the most recent match stored to the most recent match according to the day the code was executed.
  But if there are no matches stored, it'll collect the most recent matches based on the code execution day.

  If "history" option is passed, the code will collect matches form the last 7 days according to the oldest match stored.
  If "new" option is passed, the code will collect new matches from the most recent match stored to the most recent match according to the day the code was executed. And after that, it'll collect matches form the last 7 days according to the oldest match stored.
  '''
  try:
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
        print("Invalid argument.")
  except:
    print("Argument not passed.")

spark.stop()