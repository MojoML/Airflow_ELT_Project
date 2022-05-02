import requests 
import configparser
import boto3
from pathlib import Path
from datetime import datetime 

path_to_config_file = Path(__file__).parents[2] / "pipeline.conf"

path_to_extraction_file = Path(__file__).parents[1] / "scripts" / "data" / "export_file.csv"






def ingest_data_to_s3(dataset_url:str, export_file_location:str):
    try:
        parser = configparser.ConfigParser()
        parser.read(path_to_config_file)
        access_key = parser.get("aws_boto_credentials", "access_key")
        secret_key = parser.get("aws_boto_credentials", "secret_key")
        bucket_name = parser.get("aws_boto_credentials", "bucket_name")

        url_request = requests.get(dataset_url, stream=True)

        

        ingestion_time = bytes(str(datetime.now()), "iso-8859-1")

        print("Writing Staging File...")


        is_column_flag = 0
        with open(path_to_extraction_file, "wb") as f:


            for content in url_request.iter_lines():
                if is_column_flag == 0:
                    columns = content+b",ingestion_time"
                    f.write(columns)
                    f.write(b"\n")
                    is_column_flag +=1
                else:
                    row = content+b","+ingestion_time
                    f.write(row)
                    f.write(b"\n")

                
        

        s3 = boto3.client("s3",
            aws_access_key_id = access_key,
            aws_secret_access_key = secret_key)

        print("Uploading File to S3...")
        s3_file = "yellow_tripdata_raw.csv"
        s3.upload_file(str(path_to_extraction_file),bucket_name,s3_file)

    except Exception as e:
        print("An exception occured")
        print("---------------------")
        print(f"Error:{e}")


ingest_data_to_s3("https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2019-01.csv",path_to_extraction_file )
