import requests
import json
import pandas as pd
import numpy as np
import sys
from pathlib import Path

WINDSPEED = 0.277778 # 1kph * 0.277778m/s 

def main():
    config = get_config()
    forecast_data = get_forecast(config)
    if not check_success(forecast_data):
        print("The status code returned an error.")
        return
    numpy_array = get_data_array(forecast_data)
    # print(numpy_array)
    df = get_dataframe(numpy_array)
    df = create_wind_ms_columns(df)
    fp = sys.argv[1]
    setup_path(fp)
    df.to_csv(fp,mode='w',index=False)
    sys.exit(0)

def get_data_array(forecast_data):
    hours_array = [] # Create an array that will store each day as an array.
    for i,v in enumerate(forecast_data.json()['forecast']['forecastday']):
        hour_list = forecast_data.json()['forecast']['forecastday'][i]['hour'] # Each day is here
        for k in hour_list:
            hours_array.extend([[k['time'], k['wind_kph'], k['wind_degree'], k['wind_dir'], k['gust_kph']]])
    return hours_array

def get_dataframe(data_array):
    # Create a dataframe
    return pd.DataFrame(data_array,
    columns= ['datetime','wind_kph','wind_degree','wind_dir','gust_kph'])
    
def create_wind_ms_columns(df):
    # Transform df to have meters/second for ease of calculations later.
    df['wind_ms'] = round(df['wind_kph'] * WINDSPEED,2) 
    df['gust_ms'] = round(df['gust_kph'] * WINDSPEED,2)
    return df
 
def get_forecast(config):
    return requests.get(url=config["API_FORECAST"],params=dict(q=config["Location"],key=config["API_KEY"],days=7))

def check_success(request):
    return request.status_code == 200

def get_config():
    config_file = dict({})
    with open("../data/config.json") as f:
        config_file = json.load(f)
    return config_file

def setup_path(path):
    path = Path(path)
    path.parent.mkdir(parents=True,exist_ok=True)


if __name__ == "__main__":
    main()