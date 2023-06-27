from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from plugins import async_crawler
from airflow.decorators import task # decorator 임포트

import time
import asyncio
import os


@task
def scrape_user(url, start, end) -> list:
    base_url = url
    start = start
    end = end
    start_time = time.time()
    scraper = async_crawler.Scraper(flag='user')
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scraper.get_object_thread(base_url, start, end))
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


default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'async_baekjoon_user_scraper',
    default_args=default_args,
    description='A async user scraper DAG',
    schedule_interval='@once',
) as dag:
    
    url = "https://www.acmicpc.net/ranklist/"
    start = 1
    end = 1000

    scraper_objects = scrape_user(url, start, end)
    save_csv_task = save_to_csv(scraper_objects)
    # Define dependencies
    scraper_objects >> save_csv_task