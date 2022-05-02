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


sql = """
CREATE OR REPLACE TABLE taxi_trips_cleaned AS
    
    WITH renamed_columns AS (
SELECT 
    "VendorID" AS vendor_id,
    "tpep_pickup_datetime" AS pick_up_datetime,
    "tpep_dropoff_datetime" dropoff_datetime,
    "passenger_count" passenger_count,
    "trip_distance" AS trip_distance_miles,
    "RatecodeID" AS rate_code_id,
    "store_and_fwd_flag" store_and_fwd_flag,
    "PULocationID" AS pu_location_id,
    "DOLocationID" AS do_location_id,
    "payment_type" AS payment_type_id,
    "fare_amount" fare_amount,
    "extra" extra_surcharges,
    "mta_tax" mta_tax,
    "tip_amount" tip_amount,
    "tolls_amount" tolls_amount,
    "improvement_surcharge" improvement_surcharge,
    "total_amount" total_amount,
    "congestion_surcharge" congestion_surcharge,
    "ingestion_time" ingestion_time
FROM raw_taxi_trips

),
cleaned_data AS (
SELECT 
    CAST(vendor_id AS INT) vendor_id,
    CAST(pick_up_datetime AS DATETIME) pick_up_datetime,
    CAST(dropoff_datetime AS DATETIME) dropoff_datetime,
    CAST(NULLIF(passenger_count, 0) AS INT) passenger_count,
    CAST(trip_distance_miles AS FLOAT) trip_distance_miles,
    CAST(CASE WHEN store_and_fwd_flag = 'N'
        THEN 0
        ELSE 1
    END AS BOOLEAN) store_and_fwd_flag,
    CAST(pu_location_id AS INT) pu_location_id,
    CAST(do_location_id AS INT) do_location_id,
    CAST(REPLACE(fare_amount, '-', '') AS FLOAT) fare_amount,
    CAST(REPLACE(extra_surcharges, '-', '') AS FLOAT) extra_surcharges,
    CAST(REPLACE(mta_tax, '-', '') AS FLOAT) mta_tax,
    CAST(REPLACE(tip_amount, '-', '') AS FLOAT) tip_amount,
    CAST(REPLACE(tolls_amount, '-', '') AS FLOAT) tolls_amount,
    CAST(REPLACE(improvement_surcharge, '-', '') AS FLOAT) improvement_surcharge,
    CAST(REPLACE(total_amount, '-', '') AS FLOAT) total_amount,
    CAST(COALESCE(congestion_surcharge, 0, NULL) AS FLOAT) congestion_surcharge,
    CAST(ingestion_time AS DATETIME) ingestion_time
FROM renamed_columns
WHERE datediff(DAY, pick_up_datetime, dropoff_datetime) < 1
 )
SELECT * FROM cleaned_data;
"""

cur = snow_conn.cursor()
cur.execute(sql)
cur.close()