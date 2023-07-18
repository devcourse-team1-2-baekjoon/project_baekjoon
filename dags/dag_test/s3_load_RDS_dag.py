from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os

# This dag should be triggered after Athena Dag is finished

# s3_load_RDS_dag
## Task1: list_s3_files: get filenames from s3 bucket
## Task2: load_csv_to_postgres: load csv files to postgres
## Dag: task1 >> task2

@task
def list_s3_files():
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    files = s3_hook.list_keys(bucket_name='bucket_name')
    csv_files = [file for file in files if file.endswith('.csv')]
    return csv_files

@task
def load_csv_to_postgres(filename):
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    postgres_hook = PostgresHook(postgres_conn_id='postgres_conn_id')

    file_obj = s3_hook.get_key(
        key=filename,
        bucket_name='bucket_name'
    )

    data = file_obj.get()['Body'].read().decode('utf-8')
    rows = data.split('\n')
    rows = rows[1:]
    rows = [tuple(row.split(',')) for row in rows]
    table_name = os.path.splitext(filename)[0]

    for row in rows:
        postgres_hook.run(f"INSERT INTO raw_data.{table_name} VALUES {row};")

with DAG(
    dag_id = 's3_load_postgre_dag', 
    start_date=datetime(2023, 7, 18),
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
    ) as dag:

    filenames = list_s3_files()
    load_tasks = [load_csv_to_postgres(filename) for filename in filenames]

    filenames >> load_tasks


