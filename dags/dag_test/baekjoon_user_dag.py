from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import concurrent.futures
from plugins import crawler
from airflow.decorators import task # decorator 임포트
import time
import os


@task
def scrape_user(url, start, end) -> list:

    start_time = time.time()
    scraper = crawler.Scraper(flag='user')
    scraper.get_object_thread(base_url=url, start=start, end=end)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     start_indexes = list(range(1, 271, 54))  
    #     end_indexes = start_indexes[1:] + [271]
    #     executor.map(scraper.get_problems_thread, base_url, start_indexes, end_indexes)
    # context['ti'].xcom_push(key='user_scraper', value=scraper.users)
    
    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Task 실행 시간: {execution_time}초")
    
    return scraper.users

@task
def save_to_csv(scraper_objects:list) -> None:
    scraper = crawler.Scraper(flag='user')
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
    'baekjoon_user_scraper',
    default_args=default_args,
    description='A user scraper DAG',
    schedule_interval='@once',
) as dag:
    
    url = "https://www.acmicpc.net/ranklist/"
    start = 1
    end = 1000

    scraper_objects = scrape_user(url, start, end)
    save_to_csv(scraper_objects)
    # Define dependencies
    # scrape_users >> save_csv