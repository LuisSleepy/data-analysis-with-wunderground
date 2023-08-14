# Import necessary libraries
import pandas as pd
import os
import requests
import time
from pandas import json_normalize
from datetime import datetime, timedelta
from dotenv import find_dotenv, load_dotenv

# Get the API key
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)
API_KEY = os.getenv("API_KEY")

# MODIFY HERE!
start_date_request = "YYYY-MM-DD"   # Initial date of request
end_date_request = "YYYY-MM-DD"     # Final date of request (inclusive)
station = "STATION_ID"               # Station name. This is available in Wunderground.

start_date_request_datetime = datetime.strptime(start_date_request, "%Y-%m-%d")
end_date_request_datetime = datetime.strptime(end_date_request, "%Y-%m-%d")

# Iterate from the starting date until the end date of the request
recurring_datetime = start_date_request_datetime
while recurring_datetime <= end_date_request_datetime:
    current_datetime = recurring_datetime.strftime("%Y%m%d")

    url = "https://api.weather.com/v2/pws/history/all?stationId=" + station + "&format=json&units=m&date=" + current_datetime + '&apiKey=' + API_KEY
    response = requests.get(url)

    try:
        data = response.json()
        df = json_normalize(data, record_path=['observations'])
        if (recurring_datetime == start_date_request_datetime):
            dataset_df = df
        else:
            dataset_df = pd.concat([dataset_df, df], axis=0, ignore_index=True)
    except Exception as e:
        print(f"An error has occured: {e}")
        
    tomorrow_datetime = recurring_datetime + timedelta(days=1)
    recurring_datetime = tomorrow_datetime
    
    # Necessary to not overload the API service and encounter failure
    time.sleep(2)

# Rename the columns. This is a project specific. 
# These new column names are those used in the database.
dataset_df = dataset_df.rename(columns={
    'stationID': 'station_id',
    'obsTimeUtc': 'obs_time_utc',
    'obsTimeLocal': 'obs_time_local',
    'lat': 'latitude',
    'lon': 'longitude',
    'solarRadiationHigh': 'solar_radiation_high',
    'uvHigh': 'uv_high',
    'winddirAvg': 'wind_direction_avg',
    'humidityHigh': 'humidity_high',
    'humidityLow': 'humidity_low',
    'humidityAvg': 'humidity_avg',
    'qcStatus': 'qc_status',
    'metric.tempHigh': 'temperature_high',
    'metric.tempLow': 'temperature_low',
    'metric.tempAvg': 'temperature_avg',
    'metric.windspeedHigh': 'wind_speed_high',
    'metric.windspeedLow': 'wind_speed_low',
    'metric.windspeedAvg': 'wind_speed_avg',
    'metric.windgustHigh': 'wind_gust_high',
    'metric.windgustLow': 'wind_gust_low',
    'metric.windgustAvg': 'wind_gust_avg',
    'metric.dewptHigh': 'dew_point_high',
    'metric.dewptLow': 'dew_point_low',
    'metric.dewptAvg': 'dew_point_avg',
    'metric.windchillHigh': 'wind_chill_high',
    'metric.windchillLow': 'wind_chill_low',
    'metric.windchillAvg': 'wind_chill_avg',
    'metric.heatindexHigh': 'heat_index_high',
    'metric.heatindexLow': 'heat_index_low',
    'metric.heatindexAvg': 'heat_index_avg',
    'metric.pressureMax': 'pressure_max',
    'metric.pressureMin': 'pressure_min',
    'metric.pressureTrend': 'pressure_trend',
    'metric.precipRate': 'precipitation_rate',
    'metric.precipTotal': 'precipitation_total'
})

# Save the dataset as a CSV file
file_name_csv = station + "_" + start_date_request + "_" + end_date_request + "_observations_data.csv"
dataset_df.to_csv(file_name_csv)