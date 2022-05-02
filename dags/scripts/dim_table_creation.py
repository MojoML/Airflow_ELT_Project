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




create_dim_vendor = """

CREATE OR REPLACE TABLE dim_vendor(
    vendor_id INT NOT NULL,
    vendor_name VARCHAR,
    PRIMARY KEY (vendor_id));
"""

insert_dim_vendor = """

INSERT INTO dim_vendor (vendor_id, vendor_name)
(SELECT distinct vendor_id, 
    CASE 
        WHEN vendor_id = 1
            THEN 'Creative Mobile Technologies'
        WHEN vendor_id = 2
            THEN 'VeriFone Inc.'
        ELSE 'unknown'
        END AS vendor_name
FROM taxi_trips_cleaned);

"""

create_dim_trip_time = """

CREATE OR REPLACE TABLE dim_trip_time(
    trip_time_id INT AUTOINCREMENT,
    pick_up_datetime DATETIME,
    pick_up_day VARCHAR, 
    pick_up_month VARCHAR,
    pick_up_hour INT,
    dropoff_datetime DATETIME,
    dropoff_day VARCHAR, 
    dropoff_month VARCHAR,
    dropoff_hour INT,
    duration_minutes INT,
    PRIMARY KEY (trip_time_id));


"""

insert_dim_trip_time = """

INSERT INTO dim_trip_time (pick_up_datetime, pick_up_day, pick_up_month, pick_up_hour, dropoff_datetime, dropoff_day, dropoff_month, dropoff_hour, duration_minutes)
(SELECT 
    pick_up_datetime, 
    DAYNAME(pick_up_datetime) pick_up_day,
    MONTHNAME(pick_up_datetime) pick_up_month,
    HOUR(pick_up_datetime) pick_up_hour,
    dropoff_datetime,  
    DAYNAME(dropoff_datetime) dropoff_day,
    MONTHNAME(dropoff_datetime) dropoff_month,
    HOUR(dropoff_datetime) dropoff_hour,
    timestampdiff(minute, pick_up_datetime, dropoff_datetime) duration_minutes
FROM taxi_trips_cleaned);

"""

create_dim_locations = """

CREATE OR REPLACE TABLE dim_locations(
    "LocationID" VARCHAR,
    "Borough" VARCHAR,
    "Zone" VARCHAR,
    "service_zone" VARCHAR);
"""

copy_into_dim_locations = """
COPY INTO RAW.dim_locations
    FROM @my_s3_stage/taxi+_zone_lookup.csv
    ON_ERROR = "continue";

"""

create_final_locations = """
CREATE OR REPLACE TABLE dim_locations 
AS (
    SELECT 
        CAST("LocationID" AS INT) location_id,
        REPLACE("Borough", '"', '') borough,
        REPLACE("Zone", '"', '') zone,
        REPLACE("service_zone", '"', '') service_zone
FROM dim_locations

);

"""



cur = snow_conn.cursor()
cur.execute(create_dim_vendor)
cur.execute(insert_dim_vendor)
cur.execute(create_dim_trip_time)
cur.execute(insert_dim_trip_time)
cur.execute(create_dim_locations)
cur.execute(copy_into_dim_locations)
cur.execute(create_final_locations)
cur.close()