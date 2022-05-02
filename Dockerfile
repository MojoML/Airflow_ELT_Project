FROM apache/airflow:2.3.0-python3.8
USER airflow

COPY . ./

RUN pip install -r requirements.txt