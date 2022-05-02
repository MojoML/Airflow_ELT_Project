# Solution Architecture ELT

![alt_text](https://github.com/MojoML/Airflow_ELT_Project/blob/master/static/erd.png)

## Ingestion Layer

As a first step the Dataset is fetched from the HTTP response, staged locally and loaded into
an s3 Bucket in raw form.

## Loading Layer

The raw data is then loaded into Snowflake, where it is prepared for dimensional modeling.

## Transformation Layer

First the raw dataset is transformed into a a processed version with the following steps:

1. Renaming columns to appropriate names
2. Removing wrong characters in strings
3. Finalizing schema for the dataset

Next the data is prepared for dimensional modeling in the DWH. The fact table is created and
additional information is put into dimension tables. To see the final data model refer to
the static folder in this directory.

Finally, for BI purposes several Mart tables are created that can be used to answer specific
business inquiries or serve for strategic data-driven decision making.

## Orchestration Layer

All above mentioned tasks are orchestrated with Airflow.


## To reproduce

1. Initialize an s3 Bucket
2. In the pipeline.conf file include all needed credentials for Snowflake and AWS
3. run `docker-compose build` in this directory to build the Apache Airflow images
4. run `docker-compose up` to spin up all containers
5. Access the Airflow UI at localhost:8080
6. In the UI configure an S3 Connection
7. Run the DAg and let it do its magic :)
