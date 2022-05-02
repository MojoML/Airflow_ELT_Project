# Solution Architecture ELT

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

## Answer to Questions

1. Refer to the static folder to see the physical ERD

2. There were several data quality issues including: unwanted values in columns and outliers
   regarding trip duration times. There were trips lasting for longer then 24h. I decided that it
   was reasonable to exclude these entries from further processing. Furthermore, it would have been
   a good idea to run test scripts checking if the quality of data is sufficient. I have ommited this
   for time reasons

3. The data pipeline is explained in the first sections

4. After the initial full refresh of the data, it probably makes sense to schedule an incremental load
   into the DWH where at the end of each day all new trips are loaded and processed in the DWH, so that
   the reporting team can always use the up-to-date version for their needs.

## To reproduce

1. Initialize an s3 Bucket
2. In the pipeline.conf file include all needed credentials for Snowflake and AWS
3. run `docker-compose build` in this directory to build the Apache Airflow images
4. run `docker-compose up` to spin up all containers
5. Access the Airflow UI at localhost:8080
6. In the UI configure an S3 Connection
7. Run the DAg and let it do its magic :)
