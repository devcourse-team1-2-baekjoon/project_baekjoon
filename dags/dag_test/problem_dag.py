from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import concurrent.futures

from plugins import crawler
from airflow.decorators import task # decorator 임포트

import os

@task
def scrape_problems(url:str, start:int, end:int) -> list:
    base_url = url
    start = start
    end = end
    scraper = crawler.Scraper(flag='problem')
    scraper.get_object_thread(base_url, start, end)

    return scraper.problems

@task
def save_to_csv(scraper_objects:list) -> None:
    scraper = crawler.Scraper(flag='problem')
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
    dag_id='problem_scraper',
    default_args=default_args,
    description='A problem_scraper DAG',
    schedule_interval='@once',
) as dag:
    
    
    
    

    url = "https://www.acmicpc.net/problemset/"
    start = 1
    end = 270

    scraper_objects = scrape_problems(url, start, end)
    save_to_csv(scraper_objects)

    # # Define dependencies
    # scrape_problems_task >> save_to_csv_task
    
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     start_indexes = list(range(1, 271, 54))  
    #     end_indexes = start_indexes[1:] + [271]
    #     executor.map(scraper.get_problems_thread, base_url, start_indexes, end_indexes)
    # context['ti'].xcom_push(key='problem_scraper', value=scraper.problems)