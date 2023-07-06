from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import time
import requests
import pandas as pd
from fake_useragent import UserAgent

@task
def collect_data_and_save_to_csv():
    url = "https://solved.ac/api/v3/search/user"
    ua = UserAgent()
    headers = {"User-agent": ua.random}

    querystring = {"query": '', "page": 0}
    count = requests.get(url, headers=headers, params=querystring).json()['count']
    max_page = count // 50 + 1
    csv_file = os.path.join(os.getcwd(), 'users_detail.csv')
    start_time = time.time()

    try:
        page = 1
        while page <= max_page:
            if page % 100 == 0:
                headers = {"User-agent": ua.random}

            querystring = {"query": '', "page": page}
            response = requests.get(url, headers=headers, params=querystring)
            data = response.json()
            items = data['items']

            for item_index, item in enumerate(items):
                if item['tier'] == 6:
                    return

                filtered_item = {
                    'user_id': item['handle'],
                    'user_answer_num': item['solvedCount'],
                    'user_tier': item['tier'],
                    'user_rating': item['rating'],
                    'user_ratingByProblemsSum': item['ratingByProblemsSum'],
                    'user_ratingByClass': item['ratingByClass'],
                    'user_ratingBySolvedCount': item['ratingBySolvedCount'],
                    'user_ratingByVoteCount': item['ratingByVoteCount'],
                    'user_class': item['class'],
                    'user_maxStreak': item['maxStreak'],
                    'user_joinedAt': item['joinedAt'],
                    'user_rank': item['rank']
                }

                df = pd.DataFrame([filtered_item])
                is_first_item = (page == 1 and item_index == 0)

                if is_first_item:
                    df.to_csv(csv_file, index=False, encoding='utf-8')
                else:
                    df.to_csv(csv_file, mode='a', header=False, index=False, encoding='utf-8')

            page += 1

    except Exception as e:
        print(f"An error occurred: {e}")
        return

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Task 실행 시간: {execution_time}초")
    print("Data collection and saving completed successfully.")

@task
def upload_local_file_to_s3():
    local_file_path = os.path.join(os.getcwd(),'users_detail.csv')
    s3_bucket_name = 's3_bucket_name'
    s3_file_key = 'engine/users_detail.csv'
    s3_hook = S3Hook(aws_conn_id='aws_conn_id')
    s3_hook.load_file(
        filename=local_file_path,
        key=s3_file_key,
        bucket_name=s3_bucket_name,
        replace=True
    )

with DAG(
    dag_id = 'solved_ac_data_collect',
    start_date = datetime(2023,7,4), 
    schedule_interval = '0 0 * * *',  
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
}
) as dag:

    data_collection_task = collect_data_and_save_to_csv()
    upload_task = upload_local_file_to_s3()

    data_collection_task >> upload_task
