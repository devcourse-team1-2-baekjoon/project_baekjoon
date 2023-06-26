import requests
import json
import csv
from datetime import datetime, timedelta
import time

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task

default_args = {
    'owner': 'airflow',
    'catchup':False,
    'start_date': datetime(2023, 6, 26),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('solvedac_tags_pipeline', default_args=default_args, schedule_interval=timedelta(weeks=1))

@task
def fetch_data(page):
    url = f'https://solved.ac/api/v3/search/tag'
    headers = {'Accept': 'application/json'}
    params = {'page': page}
    
    response = requests.get(url, headers=headers, params=params)
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        return None


@task
def save_data_to_csv(data, file_path):
    if data is not None:
        with open(file_path, 'a', newline='') as file:
            writer = csv.writer(file)
            for item in data['items']:
                tag_id = item['bojTagId']
                display_name = item['displayNames'][0]['name']
                alias = item['aliases'][0] if item['aliases'] else ''
                writer.writerow([tag_id, display_name, alias])


@task
def fetch_and_save_data(page):
    data = fetch_data(page)
    if data is not None:
        file_path = f'/path/to/save/data.csv'
        save_data_to_csv(data, file_path)


@task
def run_data_pipeline():
    for page in range(1, 5):
        fetch_and_save_data(page)
        time.sleep(1)

num_pages = 4

fetch_tasks = []
for page in range(1, num_pages + 1):
    task_id = f'fetch_task_{page}'
    fetch_task = PythonOperator(
        task_id=task_id,
        python_callable=fetch_and_save_data,
        op_kwargs={'page': page},
        dag=dag
    )
    fetch_tasks.append(fetch_task)

# save_tasks 생성
save_tasks = []
for page in range(1, num_pages + 1):
    task_id = f'save_task_{page}'
    save_task = PythonOperator(
        task_id=task_id,
        python_callable=fetch_and_save_data,
        op_kwargs={'page': page},
        dag=dag
    )
    save_tasks.append(save_task)

# fetch_tasks 간의 종속성 설정
for i in range(num_pages - 1):
    fetch_tasks[i] >> fetch_tasks[i + 1]

# fetch_tasks와 save_tasks 간의 종속성 설정
for i in range(num_pages):
    fetch_tasks[i] >> save_tasks[i]

run_pipeline_task = PythonOperator(
    task_id='run_data_pipeline_task',
    python_callable=run_data_pipeline,
    dag=dag
)

# save_tasks와 run_pipeline_task 간의 종속성 설정
for save_task in save_tasks:
    save_task >> run_pipeline_task
