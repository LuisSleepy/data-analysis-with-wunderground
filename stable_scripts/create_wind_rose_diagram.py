# Import necessary libraries
import plotly.express as px
import polars as pl
import pandas as pd
import os
from dotenv import find_dotenv, load_dotenv

# Get the necessary variables for the database connection
dotenv_path = find_dotenv()
load_dotenv(dotenv_path)

POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_SERVER = os.getenv("POSTGRES_SERVER")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")

### USER INPUTS HERE
STATION_ID = 'IPARAA10'		# Weather station ID as indicated in Wunderground



# Query for retrieving data
# Use single quotation marks in the query for the column values, like the one in the WHERE clause
query = f"""
WITH latest_date_cte AS(
SELECT
	station_id,
	DATE(MAX(obs_time_local)) AS latest_date
FROM measurements
	
GROUP BY station_id

)

SELECT
	t1.station_id,
	DATE(t1.obs_time_local) AS obs_date_local,
    t1.obs_time_local,
	t1.wind_direction_avg AS avg_wind_dir,
	t1.wind_gust_avg AS avg_wind_gust
FROM measurements t1
LEFT JOIN latest_date_cte t2
ON t1.station_id = t2.station_id

WHERE t1.station_id IN ('{STATION_ID}')
AND DATE(t1.obs_time_local) >= (t2.latest_date - INTERVAL '28 days')
AND t1.qc_status = 1
AND t1.wind_gust_avg > 0
"""

# Run the query and get the data from the database
connection_url = f"postgres://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
df = pl.read_database_uri(query=query, uri=connection_url)

# Add the neighborhood name for each weather station
df = df.with_columns(
    pl.when(pl.col("station_id") == 'IPARAAQU3')
    .then(pl.lit('Don Bosco Better Living, Paranaque City'))
    .when(pl.col("station_id") == 'IPARAA10')
    .then(pl.lit('Merville-Sun Valley, Paranaque City'))
    .when(pl.col("station_id") == 'IBULACAN2')
    .then(pl.lit('Pandi, Bulacan'))
    .when(pl.col("station_id") == 'IMAKAT1')
    .then(pl.lit('Poblacion, Makati City'))
    .when(pl.col("station_id") == 'IRIZBULA2')
    .then(pl.lit('Poblacion, Makati City'))
    .when(pl.col("station_id") == 'IMETROMA22')
    .then(pl.lit('Alabang, Muntinlupa City'))
    .when(pl.col("station_id") == 'IMUNTI6')
    .then(pl.lit('Tunasan, Muntinlupa City'))
    .alias("cleaned_neighborhood")
)

# Round off the wind direction (in degrees) to nearest tens
# Reduce the variability of the wind direction so the entries with close wind direction (in degrees) will be grouped together
df = df.with_columns(
    ((pl.col("avg_wind_dir") / 10).cast(pl.Int64) * 10)
    .alias("avg_wind_dir_bin")
)

# Count the occurrences of each wind direction for each day in each weather station
grouped_df = (
    df.lazy()
    .group_by("cleaned_neighborhood", "obs_date_local", "avg_wind_dir_bin")
    .agg(
        pl.count()
        .alias("occurrence")
    )
    .sort("cleaned_neighborhood", "obs_date_local", "avg_wind_dir_bin", descending=[False, False, False])
)
grouped_df = grouped_df.collect()

# Determine the frequency of each wind direction per day in each weather station
total_per_day_df = grouped_df.select(
    "cleaned_neighborhood",
    "obs_date_local",
    "avg_wind_dir_bin",
    "occurrence",
    pl.col("occurrence")
    .sum()
    .over(["cleaned_neighborhood", "obs_date_local"])
    .alias("total"),
    ((pl.col("occurrence") / (pl.col("occurrence").sum().over(["cleaned_neighborhood", "obs_date_local"]))) * 100).round(2) # The resulting frequency is in %
    .alias("frequency")
)

# Convert from Polars to Pandas for easier usage in Plotly
final_df = total_per_day_df.to_pandas()
final_df['obs_date_local'] = pd.to_datetime(final_df['obs_date_local']).dt.date
final_df.to_csv('final_data_wind_rose_diagram.csv')

# Configuring the wind rose diagram
fig = px.bar_polar(final_df,
                   r='frequency',
                   theta="avg_wind_dir_bin",
                   start_angle=90,
                   direction='clockwise',
                   template="plotly_dark",
                   color_discrete_sequence= px.colors.sequential.Plasma_r,
                   animation_frame='obs_date_local',
                   hover_data={'avg_wind_dir_bin': False},
                   labels={'obs_date_local': 'Date', 'frequency': 'Daily Frequency (%)'}
                   )

# This is to change the degrees ticks into ticks of direction
fig.update_polars(
    angularaxis_tickvals = [0, 45, 90, 135, 180, 225, 270, 315],
    angularaxis_ticktext = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
)

html_file_name = 'html-outputs/' + STATION_ID + '_daily_wind_rose_diagram_past28d-v2.html'
fig.write_html(html_file_name)