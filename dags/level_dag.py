from airflow import DAG
from airflow.operators.python import PythonOperator

from airflow.decorators import task # decorator 임포트
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta
import os
import csv

@task
def save_to_csv() -> None:
    
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "level.csv")
    
    csvfile = open(file_path, "w", newline="")
    csvwriter = csv.writer(csvfile)

    header = ['user_tier', 'tier_name']
    csvwriter.writerow(header)

    data = ['Unrated',
            'Bronze V',
            'Bronze IV',
            'Bronze III',
            'Bronze II',
            'Bronze I',
            'Silver V',
            'Silver IV',
            'Silver III',
            'Silver II',
            'Silver I',
            'Gold V',
            'Gold IV',
            'Gold III',
            'Gold II',
            'Gold I',
            'Platinum V',
            'Platinum IV',
            'Platinum III',
            'Platinum II',
            'Platinum I',
            'Diamond V',
            'Diamond IV',
            'Diamond III',
            'Diamond II',
            'Diamond I',
            'Ruby V',
            'Ruby IV',
            'Ruby III',
            'Ruby II',
            'Ruby I']
    
    for i in range(len(data)):
        row = [i, data[i]]
        csvwriter.writerow(row)

    csvfile.close()

    print(file_path)

@task
def upload_to_s3():
    local_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/level.csv")
    s3_hook = S3Hook('aws_defalut') 
    s3_bucket = 'baekjoon-project-pipeline'
    s3_key = 'CSV_load_to_S3/level.csv'
    s3_hook.load_file(
        filename = local_file_path,
        key = s3_key,
        bucket_name = s3_bucket,
        replace = True
    )

    print(f"File uploaded to S3: s3://{s3_bucket}/{s3_key}")

default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'level_csv_to_s3',
    default_args=default_args,
    description='save level info to csv and upload to s3 DAG',
    schedule_interval='@once',
) as dag:
    
    save_to_csv_task = PythonOperator(
        task_id = 'save_to_csv_task',
        python_callable = save_to_csv
    )

    upload_to_s3_task = PythonOperator(
        task_id = 'upload_to_s3_task',
        python_callable = upload_to_s3
    )
    
    # Define dependencies
    save_to_csv_task >> upload_to_s3_task
