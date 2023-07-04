from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta

from plugins import async_crawler
from airflow.decorators import task # decorator 임포트

import logging
import os
import glob


@task
def start_dag() -> None:
    logging.info("Start DAG!")
    

@task
def upload_local_file_to_s3(csv_file:str, s3_key:str, s3_bucket_name:str, s3_hook) -> None:
    logging.info("Start upload!")
    
    logging.info(f'{s3_key}' + " upload to " + f'{s3_bucket_name}')
    
    s3_hook.load_file(
        filename=csv_file,
        key= s3_key,
        bucket_name=s3_bucket_name,
        replace=True
    )

@task
def upload_message()-> None:
    logging.info("Upload complete!")


default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'problem_s3_upload_dag',
    default_args=default_args,
    description='A problem_s3_upload hook csv DAG',
    schedule_interval='@once',
) as dag:
    
    data_folder = os.path.join(os.getcwd(), "data")
    csv_file = os.path.join(data_folder, 'problems.csv')

    s3_bucket_name = 'airflow-bucket-hajun'
    s3_folder = 'baekjoon/'
    
    file_name = os.path.basename(csv_file)
    s3_key = os.path.join(s3_folder, file_name)
    
    start_task = start_dag()

    s3_hook = S3Hook(aws_conn_id='hajun_aws_conn_id')
    upload_task = upload_local_file_to_s3(csv_file=csv_file, s3_key=s3_key, s3_bucket_name=s3_bucket_name, s3_hook=s3_hook) 

    message_task = upload_message()
    
    start_task >> upload_task >> message_task