from airflow.decorators import task
from airflow import DAG
from datetime import datetime, timedelta
import os
import time
import requests
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import csv
import logging
import random

# Set up logging
logger = logging.getLogger(__name__)


@task
def collect_tags_and_save_to_csv(url:str) -> None:
    logger.info('Collecting tags and saving to CSV')
    ua_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            ]   
    headers = {'User-Agent': random.choice(ua_list)}
    
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "problem_tag.csv")

    params = {"query": '', "page": '0'}

    page = 1
    with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.writer(csv_file)
        header = ['tag_key','tag_id','tag_problem_num','tag_display_lang','tag_name', 'tag_name_short']
        writer.writerow(header)
        
        while True:
            params = {"query": '', "page": page}
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            items = data['items']

            if not items:
                break

            for item in items:
                display_names = item['displayNames']
                filtered_display_names = [name for name in display_names if name['language'] in ['ko']]

                for name in filtered_display_names:
                    new_item = item.copy()
                    new_item['displayNames_language'] = name['language']
                    new_item['displayNames_name'] = name['name']
                    new_item['displayNames_short'] = name['short']

                    new_item.pop('displayNames', None)
                    new_item.pop('aliases', None)
                    new_item.pop('isMeta', None)

                    writer.writerow(new_item.values())

            page += 1
            
    logger.info('Finished collecting tags')



@task
def upload_to_s3(aws_conn_id:str) -> None:
    s3_hook = S3Hook(aws_conn_id=aws_conn_id) # conn_id 입력
    bucket_name = 'airflow-bucket-hajun' #bucket name 입력
    local_file_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/problem_tag.csv")
    s3_key = 'problem_tag/problem_tag.csv'
    
    s3_hook.load_file(
        filename=local_file_path,
        key=s3_key,
        bucket_name=bucket_name,
        replace=True
    )
    logger.info(f"File uploaded to S3: s3://{bucket_name}/{s3_key}")



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 5, 20),
    'catchup': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}


with DAG('problem_tag_dag', 
        default_args=default_args, 
        schedule_interval='@once'
    ) as dag:

    url = "https://solved.ac/api/v3/search/tag"
    aws_conn_id = 'hajun_aws_conn_id'
    collect_tags_task = collect_tags_and_save_to_csv(url=url)
    # upload_task = upload_to_s3(aws_conn_id=aws_conn_id)

    collect_tags_task 