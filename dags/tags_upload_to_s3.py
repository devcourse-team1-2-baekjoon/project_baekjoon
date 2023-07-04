from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os


def upload_to_s3():
    s3_hook = S3Hook(aws_conn_id='problem_tags')
    bucket_name = 'juhye-baekjoon'
    local_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/problems.csv")
    s3_key = 'problems.csv'
    
    s3_hook.load_file(
        filename=local_file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 20),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


with DAG('upload_to_s3_dag',
        default_args=default_args,
        schedule_interval='@once'
    ) as dag:

    upload_to_s3_task = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    upload_to_s3_task