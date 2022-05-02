from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor


with DAG("elt_pipeline", schedule_interval="@once", start_date = days_ago(1)) as dag:

    extract_raw_data = BashOperator(
        task_id="extract_raw_data",
        bash_command = "python /opt/airflow/dags/scripts/extraction.py"

    )

    wait_for_file_on_s3 = S3KeySensor(
        task_id="waiting_for_file_on_s3",
        bucket_key= "s3://raw-kevin/yellow_tripdata_raw.csv",
        aws_conn_id= "s3_conn")


    load_raw_data = BashOperator(
        task_id= "load_raw_data",
        bash_command = "python /opt/airflow/dags/scripts/loading.py"
    )

    clean_raw_date = BashOperator(
        task_id="clean_raw_data",
        bash_command="python /opt/airflow/dags/scripts/transformation.py"
    )

    create_dim_tables = BashOperator(
        task_id="create_dim_tables",
        bash_command="python /opt/airflow/dags/scripts/dim_table_creation.py"
    )

    create_fact_table = BashOperator(
        task_id="create_fact_table",
        bash_command="python /opt/airflow/dags/scripts/fct_table_creation.py"
    )

    create_mart_tables = BashOperator(
        task_id="create_mart_tables",
        bash_command="python /opt/airflow/dags/scripts/mart_table_creation.py"
    )


    extract_raw_data >> wait_for_file_on_s3 >> load_raw_data >> clean_raw_date >> create_dim_tables >> create_fact_table >> create_mart_tables