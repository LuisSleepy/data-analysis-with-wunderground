import polars as pl
import psycopg2
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

# REVISE HERE
# Query for retrieving data
connection_url = f"postgres://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_SERVER}:{POSTGRES_PORT}/{POSTGRES_DATABASE}"
# Use single quotation marks in the query for the column values, like the one in the WHERE clause
query = f"""
WITH latest_date_cte AS(
SELECT
	station_id,
	DATE(MAX(obs_time_local)) AS latest_date
FROM measurements
	
GROUP BY station_id

), obs_met_data AS (
	SELECT
		t1.station_id AS station_id,
		DATE(t1.obs_time_local) AS obs_date,
		MIN(t1.temperature_avg) AS min_temp,
		MIN(t1.heat_index_avg) AS min_heat_index,
		MIN(t1.wind_chill_avg) AS min_wind_chill,
		MAX(t1.temperature_avg) AS max_temp,
		MAX(t1.heat_index_avg) AS max_heat_index,
		MAX(t1.wind_chill_avg) AS max_wind_chill,
        AVG(t1.temperature_avg) AS avg_temp,
        AVG(t1.heat_index_avg) AS avg_heat_index,
		AVG(t1.wind_direction_avg) as avg_wind_dir
	FROM measurements t1

	LEFT JOIN latest_date_cte t2
	ON t1.station_id = t2.station_id

	WHERE DATE(t1.obs_time_local) >= (t2.latest_date - INTERVAL '28 days')
	AND t1.station_id IN ('IPARAA10', 'IMUNTI6')
	AND t1.qc_status = 1

	GROUP BY t1.station_id, obs_date

)

SELECT
	x.station_id,
	t6.neighborhood,
	t6.country,
	t6.latitude,
	t6.longitude,
	x.obs_date,
	x.min_temp,
	MIN(t3.obs_time_local) AS min_temp_obs_time,
	x.min_heat_index,
	MIN(t4.obs_time_local) AS min_heat_index_obs_time,
	x.min_wind_chill,
	MIN(t5.obs_time_local) AS min_wind_chill_obs_time,
    x.avg_temp,
    x.avg_heat_index,
	x.avg_wind_dir,
	x.max_temp,
	x.max_heat_index,
	x.max_wind_chill

FROM obs_met_data x

LEFT JOIN measurements t3
ON x.station_id = t3.station_id
AND x.obs_date = DATE(t3.obs_time_local)
AND x.min_temp = t3.temperature_avg

LEFT JOIN measurements t4
ON x.station_id = t4.station_id
AND x.obs_date = DATE(t4.obs_time_local)
AND x.min_heat_index = t4.heat_index_avg

LEFT JOIN measurements t5
ON x.station_id = t5.station_id
AND x.obs_date = DATE(t5.obs_time_local)
AND x.min_wind_chill = t5.wind_chill_avg

LEFT JOIN stations t6
ON x.station_id = t6.station_id

GROUP BY x.station_id, t6.neighborhood, t6.country, t6.latitude, t6.longitude, x.obs_date, x.min_temp, x.min_heat_index, x.min_wind_chill, x.avg_temp, x.avg_heat_index, x.avg_wind_dir, x.max_temp, x.max_heat_index, x.max_wind_chill
ORDER BY obs_date ASC
"""

df = pl.read_database_uri(query=query, uri=connection_url)

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

df = df.with_columns(
    pl.when(pl.col("obs_date").dt.month() == 1)
    .then(pl.lit(value=23.9, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 2)
    .then(pl.lit(value=24.3, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 3)
    .then(pl.lit(value=25.3, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 4)
    .then(pl.lit(value=26.7, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 5)
    .then(pl.lit(value=27.0, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 6)
    .then(pl.lit(value=26.5, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 7)
    .then(pl.lit(value=25.9, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 8)
    .then(pl.lit(value=25.9, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 9)
    .then(pl.lit(value=25.7, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 10)
    .then(pl.lit(value=25.7, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 11)
    .then(pl.lit(value=25.3, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 12)
    .then(pl.lit(value=24.6, dtype=pl.Float64))
    .alias("normal_min_temp")
)

df = df.with_columns(
    pl.when(pl.col("obs_date").dt.month() == 1)
    .then(pl.lit(value=29.9, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 2)
    .then(pl.lit(value=30.7, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 3)
    .then(pl.lit(value=32.1, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 4)
    .then(pl.lit(value=33.8, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 5)
    .then(pl.lit(value=33.6, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 6)
    .then(pl.lit(value=32.8, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 7)
    .then(pl.lit(value=31.5, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 8)
    .then(pl.lit(value=31.0, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 9)
    .then(pl.lit(value=31.2, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 10)
    .then(pl.lit(value=31.4, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 11)
    .then(pl.lit(value=31.3, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 12)
    .then(pl.lit(value=30.3, dtype=pl.Float64))
    .alias("normal_max_temp")
)

df = df.with_columns(
    pl.when(pl.col("obs_date").dt.month() == 1)
    .then(pl.lit(value=26.9, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 2)
    .then(pl.lit(value=27.5, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 3)
    .then(pl.lit(value=28.7, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 4)
    .then(pl.lit(value=30.3, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 5)
    .then(pl.lit(value=30.3, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 6)
    .then(pl.lit(value=29.7, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 7)
    .then(pl.lit(value=28.7, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 8)
    .then(pl.lit(value=28.5, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 9)
    .then(pl.lit(value=28.4, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 10)
    .then(pl.lit(value=28.6, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 11)
    .then(pl.lit(value=28.3, dtype=pl.Float64))
    .when(pl.col("obs_date").dt.month() == 12)
    .then(pl.lit(value=27.4, dtype=pl.Float64))
    .alias("normal_avg_temp")
)

df.write_csv(file='output-datasets/latest-28d-min-max-temp-and-related-met-params-v3.csv')