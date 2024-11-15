import api_key
import requests
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType
from datetime import datetime, timedelta, date
import traceback

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
  if response.json():
    print(f'Partida(s) do dia {date} coletada(s)')
  else:
    print(f'Não há partida(s) no dia {date}')
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
    if match:
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

def load_date(date):
  '''
  Function to load matches from a specific date.
  '''
  match = get_match(date=date)
  if match:
    df_match = spark.createDataFrame(match, schema=schema)
    save_match_list(df_match)

def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, '%Y-%m-%d')
        return True
    except ValueError:
        return False
    
def load_range_date(date1, date2):
  while date1 <= date2:
    match = get_match(date=date1)
    if match:
      df_match = spark.createDataFrame(match, schema=schema)
      save_match_list(df_match)
    date1 = (datetime.strptime(date1,'%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')

if __name__ == '__main__':
  '''
  When this function is directly called, a parameter with two options ("new" or "history") determines the code execution.

  If "new" option is passed, the code will collect new matches from the most recent match stored to the most recent match according to the day the code was executed.
  But if there are no matches stored, it'll collect the most recent matches based on the code execution day.

  If "history" option is passed, the code will collect matches from the last 7 days according to the oldest match stored.
  But if there are no matches stored, it'll collect the most recent matches based on the code execution day. And after that, it will collect matches from the last 7 days according to the oldest match stored.

  If "date" option is passed, it expects a date in format "%YYYY-%mm-%dd" as another argument, so it tries to collect matches of that day.

  If "range" option is passed, it expects two dates in format "%YYYY-%mm-%dd" as others arguments, so it tries to collect matches between those dates. The first date must be before the second date.
  '''
  try:
    match sys.argv[1]:
      case "new":
        if not os.path.isdir("raw/csgo_match_history"):
          today = date.today().strftime('%Y-%m-%d')
          first_load(today)
        else:
          get_new_matches()
      case "history":
        if not os.path.isdir("raw/csgo_match_history"):
          today = date.today().strftime('%Y-%m-%d')
          first_load(today)
        get_history_matches()
      case "date":
        try: 
          _date = sys.argv[2]
          if is_valid_date(_date):
            load_date(_date)
          else:
            print("Data inválida.")
        except IndexError:
          print("Argumento de data não foi passado.")
      case "range":
        try:
          date1 = sys.argv[2]
          date2 = sys.argv[3]
          if is_valid_date(date1) and is_valid_date(date2):
            if (datetime.strptime(date1, "%Y-%m-%d") < datetime.strptime(date2, "%Y-%m-%d")):
              load_range_date(date1, date2)
            else:
              print("Data(s) inválida(s).")
          else:
            print("Data(s) inválida(s).")
        except IndexError:
          print("Argumento (s) de data(s) não foi(foram) passada(s).")
      case _:
        print("Argumento inválido.")
  except IndexError:
    print("Argumento não foi passado.")
  except Exception as e:
    traceback.print_exc()

spark.stop()