from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

from plugins import async_crawler
from airflow.decorators import task # decorator 임포트

import asyncio
import os

@task
def scrape_problems(url:str, start:int, end:int) -> list:
    base_url = url
    start = start
    end = end
    scraper = async_crawler.Scraper(flag='problem')
    
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scraper.get_object_thread(base_url, start, end))
    loop.close()
    
    # scraper.get_object_thread(base_url, start, end)

    return scraper.problems

@task
def save_to_csv(scraper_objects:list) -> None:
    scraper = async_crawler.Scraper(flag='problem')
    # scraper_objects = context['ti'].xcom_pull(key='problem_scraper')
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "problems.csv")
    scraper.save_to_csv(objects = scraper_objects, file_name=file_path)
    print(file_path)


default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    dag_id='async_problem_scraper',
    default_args=default_args,
    description='A async problem_scraper DAG',
    schedule_interval='@once',
) as dag:
    
    trigger_s3_upload = TriggerDagRunOperator(
        task_id="trigger_problem_s3_upload_dag",
        trigger_dag_id="problem_s3_upload_dag",
        reset_dag_run=True, # True일 경우 해당 날짜가 이미 실행되었더라도 다시 재실행
        wait_for_completion=True # DAG B가 끝날 때까지 기다릴지 여부를 결정. 디폴트값은 False
    )
    

    url = "https://www.acmicpc.net/problemset/"
    start = 1
    end = 270

    scraper_objects = scrape_problems(url, start, end)
    save_to_csv_task = save_to_csv(scraper_objects)
    
    scraper_objects >> save_to_csv_task >> trigger_s3_upload

