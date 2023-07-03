from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from datetime import datetime, timedelta

from plugins import async_crawler
from airflow.decorators import task # decorator 임포트

import time
import asyncio
import os


@task
def scrape_workbook_user(url:str, start:int, end:int, flag:str) -> list:
    base_url = url
    start = start
    end = end
    print('start scrape_workbook_user task')
    start_time = time.time()
    
    scraper = async_crawler.Scraper(flag=flag)
    loop = asyncio.get_event_loop()
    loop.run_until_complete(scraper.get_object_thread(base_url = base_url, start = start, end = end))
    loop.close()     
    
    end_time = time.time()
    print(f'scrape_workbook_user {start_time-end_time} seconds')
    return scraper.workbooks


@task
def save_to_csv(scraper_object:list, flag:str) -> None:
    
    print('start save_to_csv task')
    scraper = async_crawler.Scraper(flag=flag)
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, f"workbooks.csv")
    scraper.save_to_csv(objects=scraper_object, file_name=file_path)
    print('end save_to_csv task')
    
    
default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'max_active_tasks' : 5
}

with DAG(
    'async_workbook_scraper',
    default_args=default_args,
    description='A async workbook scraper DAG',
    schedule_interval='@once',
) as dag:
    
    trigger_s3_upload = TriggerDagRunOperator(
        task_id="trigger_workbook_s3_upload_dag",
        trigger_dag_id="workbook_s3_upload_dag",
        reset_dag_run=True, # True일 경우 해당 날짜가 이미 실행되었더라도 다시 재실행
        wait_for_completion=True # trigger_s3_upload가 끝날 때까지 기다릴지 여부를 결정. 디폴트값은 False
    )
    
    url = "https://www.acmicpc.net/workbook"
    flag = 'workbook'

    start, end = 1, 20
    scraper_task = scrape_workbook_user(url, start, end, flag)
    save_task = save_to_csv(scraper_object=scraper_task, flag=flag)

    scraper_task >> save_task >> trigger_s3_upload
