from airflow.decorators import task
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import csv
import time
import requests
from fake_useragent import UserAgent


@task
def collect_tags_and_save_to_csv():
    ua = UserAgent()
    url = "https://solved.ac/api/v3/search/tag"
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "problem_tags.csv")

    headers = {"User-agent": ua.random}

    params = {"query": '', "page": '0'}

    page = 1
    with open(file_path, 'w', encoding='utf-8', newline='') as csv_file:
        writer = csv.writer(csv_file)
        header = ['key','bojTagId','displayNames_language', 'displayNames_name', 'displayNames_short']
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


    collect_data_task = collect_tags_and_save_to_csv()

    collect_data_task