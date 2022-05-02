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


create_temp_fct = """

CREATE OR REPLACE TABLE fct_taxi_trips AS
(SELECT 
    ttc.vendor_id,
    dtt.trip_time_id,
    ttc.passenger_count,
    ttc.trip_distance_miles,
    ttc.store_and_fwd_flag,
    ttc.pu_location_id,
    ttc.do_location_id,
    ttc.fare_amount,
    ttc.extra_surcharges,
    ttc.mta_tax,
    ttc.tip_amount,
    ttc.tolls_amount,
    ttc.improvement_surcharge,
    ttc.total_amount,
    ttc.congestion_surcharge,
    ttc.ingestion_time
FROM taxi_trips_cleaned ttc
    INNER JOIN dim_trip_time dtt
    ON ttc.pick_up_datetime = dtt.pick_up_datetime 
    AND ttc.dropoff_datetime = dtt.dropoff_datetime);
    
    
"""

create_seq = """
CREATE OR REPLACE SEQUENCE seq1 start=1 increment=1;
"""

create_final_fct = """
CREATE OR REPLACE TABLE fct_taxi_trips_final LIKE fct_taxi_trips;
"""

add_trip_id = """
ALTER TABLE fct_taxi_trips_final 
ADD COLUMN trip_id int DEFAULT seq1.nextval;
"""

insert_trip_id = """
INSERT INTO fct_taxi_trips_final
SELECT *, seq1.nextval 
FROM fct_taxi_trips;
"""

cur = snow_conn.cursor()
cur.execute(create_temp_fct)
cur.execute(create_seq)
cur.execute(create_final_fct)
cur.execute(add_trip_id)
cur.execute(insert_trip_id)
cur.close()