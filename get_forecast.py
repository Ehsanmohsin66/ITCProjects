import requests
import json
import pandas as pd
import numpy as np
import sys
# from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StringType, FloatType
from pyspark import SparkContext, SparkConf
from subprocess import Popen, PIPE
from datetime import datetime
WINDSPEED = 0.277778 # 1kph * 0.277778m/s 

def main():
    config = get_config()
    forecast_data = get_forecast(config)
    if not check_success(forecast_data):
        print("The status code returned an error.")
        return
    data_array = get_data_array(forecast_data)
    df = get_dataframe(data_array)
    df = append_and_fix_columns(df)
    # Turn the df into an RDD
    rdd = send_to_hive(df)
    # Save as a single csv file.
    sys.exit(0)

def get_data_array(forecast_data):
    hours_array = [] # Create an array that will store each day as an array.
    for i,v in enumerate(forecast_data.json()['forecast']['forecastday']):
        hour_list = forecast_data.json()['forecast']['forecastday'][i]['hour'] # Each day is here
        for k in hour_list:
            print(k)
            hours_array.extend([[k['time'], k['wind_kph'], k['wind_degree'], k['temp_c']]])
    return hours_array

def get_dataframe(data_array):
    # Create a dataframe
    return pd.DataFrame(data_array,
    columns= ['datetime','wind_kph','wa_c','tempt_c'])
    
def append_and_fix_columns(df):
    # Transform df to have meters/second for ease of calculations later.
    df['wind_speed'] = df['wind_kph'] * WINDSPEED
    df['wind_speed'] = df['wind_speed'].apply(lambda x: round(x,2))
    df = df.drop("wind_kph", axis=1)
    df['wa_c'] = df['wa_c'].apply(lambda x: float(x))
    print(df)
    return df
 
def get_forecast(config):
    return requests.get(url=config["API_FORECAST"],params=dict(q=config["Location"],key=config["API_KEY"],days=7))

def check_success(request):
    return request.status_code == 200

def get_config():
    config_file = dict({})
    with open("./data/config.json") as f:
        config_file = json.load(f)
    return config_file

def send_to_hive(df):
    sc = SparkSession.builder.appName('HDFS Importer').getOrCreate()
    rdd = sc.createDataFrame(df)
    fname = f"WEATHER-{datetime.now()}.csv"
    fpath = sys.argv[1]
    rdd.write.save(path=f"{fpath}/{fname}", format="csv", mode="overwrite")

    # Hive implementation?

    # rdd.write.mode("overwrite").format("hive").saveAsTable("windpredictionproject_jan23.weather_data")

if __name__ == "__main__":
    main()