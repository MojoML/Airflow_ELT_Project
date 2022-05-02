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
COPY INTO RAW.raw_taxi_trips
    FROM @my_s3_stage/yellow_tripdata_raw.csv
    ON_ERROR = "continue";
"""

cur = snow_conn.cursor()
cur.execute(sql)
cur.close()