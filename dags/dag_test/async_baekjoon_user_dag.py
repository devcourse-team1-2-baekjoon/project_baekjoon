from airflow import DAG

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from datetime import datetime, timedelta

from plugins import async_crawler
from airflow.decorators import task # decorator 임포트

import asyncio
import os

import logging
import time


@task
def scrape_user(url:str, start:int, end:int) -> list:
    base_url = url
    start = start
    end = end
    start_time = time.time()
    scraper = async_crawler.Scraper(flag='user')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scraper.get_object_thread(base_url=base_url, start=start, end=end))
    loop.close()     
    
    # scraper.get_object_thread(base_url=url, start=start, end=end)
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Task 실행 시간: {execution_time}초")
    
    return scraper.users

@task
def save_to_csv(scraper_objects:list) -> None:
    scraper = async_crawler.Scraper(flag='user')
    # scraper_objects = context['ti'].xcom_pull(key='user_scraper')
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "bj_users.csv")
    scraper.save_to_csv(objects=scraper_objects, file_name=file_path)
    print(file_path)


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
    'async_baekjoon_user_scraper_test',
    default_args=default_args,
    description='A async user scraper DAG',
    schedule_interval='@once',
) as dag:
    

    url = "https://www.acmicpc.net/ranklist/"
    start = 1
    end = 1000
    
    s3_bucket_name = 'airflow-bucket-hajun'
    s3_folder = 'users/'
    data_folder = os.path.join(os.getcwd(), "data")
    csv_file = os.path.join(data_folder, 'bj_users.csv')
    file_name = os.path.basename(csv_file)
    s3_key = os.path.join(s3_folder, file_name)

    scraper_objects = scrape_user(url, start, end)
    save_to_csv_task = save_to_csv(scraper_objects)
    
    s3_hook = S3Hook(aws_conn_id='hajun_aws_conn_id')
    upload_task = upload_local_file_to_s3(csv_file=csv_file, s3_key=s3_key, s3_bucket_name=s3_bucket_name, s3_hook=s3_hook) 
    
    upload_message_task = upload_message()
    
    scraper_objects >> save_to_csv_task >> upload_task >> upload_message_task
    
    
    
    # Define dependencies
    
    # trigger_s3_upload = TriggerDagRunOperator(
    #     task_id="trigger_bj_users_s3_upload_dag",
    #     trigger_dag_id="bj_users_s3_upload_dag",
    #     reset_dag_run=True, # True일 경우 해당 날짜가 이미 실행되었더라도 다시 재실행
    #     wait_for_completion=True # DAG bj_users_s3_upload_dag가 끝날 때까지 기다릴지 여부를 결정. 디폴트값은 False
    # )
    