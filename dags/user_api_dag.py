from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os
import time
import requests
import pandas as pd
import random


@task
def collect_data_and_save_to_csv(delay: float) -> None:
    '''
    티어 순위 가져오기 api 사용
    시간이 오래걸리는 DAG 
    최적화가 어려움 : solvedac 호출 제한
    '''
    
    url = "https://solved.ac/api/v3/ranking/tier"
    ua_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            ]   
    headers = {'User-Agent': random.choice(ua_list), 'Accept': 'application/json'}

    querystring = {"page": 1}
    count = requests.get(url, headers=headers, params=querystring).json()['count']
    max_page = count // 50 + 1
    
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    csv_file = os.path.join(output_folder,'users_detail.csv')
    print('start save into ', csv_file)
    start_time = time.time()

    user_list = []
    flag = True
    try:
        page = 1
        '''
        각 page마다 50개의 사용자를 가져온다
        평균적으로 2444회의 호출을 해야한다
        solved.ac api의 호출제한 => 약 15분당 300회 정도까지만 가능
        
        브론즈 5를 만나면 flag가 False로 전환
        '''
        while page <= max_page and flag:
            time.sleep(delay)
            
            if page % 100 == 0:
                headers = {'User-Agent': random.choice(ua_list), 'Accept': 'application/json'}
                print('change user agent')

            querystring = {"page": page}
            response = requests.get(url, headers=headers, params=querystring)
            print(f'{page} : {response}')
            
            # Retry logic in case of a failure.
            retries = 5
            
            while response.status_code != 200 and retries > 0:
                print(f"An error occurred: {response.status_code}. Retrying in 3 seconds.")
                time.sleep(delay)
                response = requests.get(url, headers=headers, params=querystring)
                retries -= 1
                
            if retries == 0:
                print(f"Failed to retrieve data for page {page} after 5 retries.")
                continue
            
            
            try :
                data = response.json()
                items = data['items']

                # 50번 순회
                for item_index, item in enumerate(items):
                    # 브론즈 1을 만나면 return
                    if item['tier'] == 5:
                        flag = False
                        break

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
                    user_list.append(filtered_item)

                    # # df = pd.DataFrame([filtered_item])
                    # is_first_item = (page == 1 and item_index == 0)

                    # if is_first_item:
                    #     df.to_csv(csv_file, index=False, encoding='utf-8')
                    # else:
                    #     df.to_csv(csv_file, mode='a', header=False, index=False, encoding='utf-8')
            except:
                pass

            page += 1
        
        df = pd.DataFrame(user_list)
        df.to_csv(csv_file, index=False, encoding='utf-8')

    except Exception as e:
        print(f"An error occurred: {e}")
        raise

    end_time = time.time()
    execution_time = end_time - start_time
    print(f"Task 실행 시간: {execution_time}초")
    print("Data collection and saving completed successfully.")
    print(f'{csv_file} saved')


@task
def upload_local_file_to_s3(aws_conn_id:str, s3_bucket_name:str) -> None:
    local_file_path = os.path.join(os.getcwd(),'data','users_detail.csv')
    s3_file_key = 'users_detail/users_detail.csv'
    
    s3_hook = S3Hook(aws_conn_id=aws_conn_id)
    s3_hook.load_file(
        filename=local_file_path,
        key=s3_file_key,
        bucket_name=s3_bucket_name,
        replace=True
    )

with DAG(
    dag_id = 'solved_ac_user_api',
    start_date = datetime(2023,7,4), 
    schedule_interval = '@once',  
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:
    requests_per_second = 300 / (15 * 60) 
    delay = 1 / requests_per_second
    
    aws_conn_id = 'hajun_aws_conn_id'
    s3_bucket_name = 'airflow-bucket-hajun'

    user_detail_data_collection_task = collect_data_and_save_to_csv(delay=delay)
    upload_task = upload_local_file_to_s3(aws_conn_id=aws_conn_id, s3_bucket_name=s3_bucket_name)

    user_detail_data_collection_task >> upload_task
