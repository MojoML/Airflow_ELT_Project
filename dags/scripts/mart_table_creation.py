import configparser
import snowflake.connector
from pathlib import Path


path_to_config_file = Path(__file__).parents[2] / "pipeline.conf"

parser = configparser.ConfigParser()
parser.read(path_to_config_file)

username= parser.get("snowflake_creds", "username")
password = parser.get("snowflake_creds", "password")
account_name = parser.get("snowflake_creds", "account_name")

snow_conn = snowflake.connector.connect(
    user=username,
    password=password,
    account=account_name,
    warehouse="COMPUTE_WH",
    database="TAXI_TRIPS",
    schema="RAW"
)


create_most_profitable_zones = """

CREATE OR REPLACE TABLE most_profitable_zones AS 
    (SELECT SUM(fct.total_amount) total_profit, dl.zone pick_up_zone
    FROM fct_taxi_trips_final fct 
    INNER JOIN dim_locations dl
    ON dl.location_id = fct.pu_location_id
    GROUP BY pick_up_zone
    ORDER BY total_profit DESC);

"""

create_passenger_counts_per_zone = """

CREATE OR REPLACE TABLE passenger_counts_per_zone AS 
    (SELECT fct.passenger_count passenger_count, dl.zone pick_up_zone, COUNT(*) count_passengers
    FROM fct_taxi_trips_final fct 
    INNER JOIN dim_locations dl 
    ON dl.location_id = fct.pu_location_id
    GROUP BY pick_up_zone, passenger_count
    ORDER BY count_passengers DESC);
    
"""

create_most_profitable_hour_per_zone_fct = """
CREATE OR REPLACE TABLE most_profitable_hour_per_zone AS
    (SELECT dt.pick_up_hour pick_up_hour, dl.zone pick_up_zone, SUM(fct.total_amount) total_profit
    FROM fct_taxi_trips_final fct 
    INNER JOIN dim_locations dl 
    ON dl.location_id = fct.pu_location_id
    INNER JOIN dim_trip_time dt 
    ON fct.trip_time_id = dt.trip_time_id
    GROUP BY pick_up_hour, pick_up_zone)
    ORDER BY pick_up_hour, total_profit DESC;

"""



cur = snow_conn.cursor()
cur.execute(create_most_profitable_zones)
cur.execute(create_passenger_counts_per_zone)
cur.execute(create_most_profitable_hour_per_zone_fct)
cur.close()