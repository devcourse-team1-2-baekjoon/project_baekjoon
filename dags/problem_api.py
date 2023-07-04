import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 리스트 만들기

columns = ['problemId',
 'titleKo',
 'isSolvable',
 'isPartial',
 'acceptedUserCount',
 'level',
 'votedUserCount',
 'sprout',
 'givesNoRating',
 'isLevelLocked',
 'averageTries',
 'official',
 'tags']

new_columns = ['problemId',
 'titleKo',
 'language',
 'languageDisplayName',
 'title',
 'isOriginal',
 'isSolvable',
 'isPartial',
 'acceptedUserCount',
 'level',
 'votedUserCount',
 'sprout',
 'givesNoRating',
 'isLevelLocked',
 'averageTries',
 'official',
 'tags']

columns_naming = ['problem_id',
 'problem_title',
 'problem_lang',
 'tag_display_lang',
 'tag_name',
 'problem_titles_isOriginal',
 'problem_isSolvable',
 'problem_isPartial',
 'problem_answer_num',
 'problem_level',
 'problem_votedUserCount',
 'problem_sprout',
 'problem_givesNoRating',
 'problem_isLevelLocked',
 'problem_averageTries',
 'problem_official',
 'tag_key']

def get_problem_id():
    path = os.path.join(os.getcwd(),'data','problems.csv')
    df = pd.read_csv(path)
    df['id'] = df['id'].astype(str)
    problem_id = df['id'].tolist()
    return problem_id
def get_page(problem_id):
    url = 'https://solved.ac/api/v3/problem/lookup/?problemIds='
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebkKit/537.36 (KHTML, like Gecko) Chrome/81.0.4044.122 Safari/537.36'}
    bet_url = '%2C'
    param = bet_url.join(problem_id)
    final_url = url + param
    res = requests.get(final_url, headers=headers)
    logging.debug(res)
    return res.json()

def tags_apply(x):
    result = []
    for i in x:
        result.append(i['key'])
    return result

def titles_apply(x):
    return x[0]

def transform(response_json):
    df = pd.DataFrame(response_json)
    df['titles'] = df['titles'].apply(titles_apply)
    df = pd.concat([df.drop('titles',axis=1),df['titles'].apply(pd.Series)],axis=1)
    df['tags'] = df['tags'].apply(tags_apply)
    df = df[new_columns]
    df.columns = columns_naming
    df_ex = df.explode('tag_key')
    return df_ex

@task
def get_page_all():
    csv_file = os.path.join(os.getcwd(),'data','problem_list.csv')
    problem_id = get_problem_id()
    for i in range(len(problem_id) // 100 + 1):
        start = i * 100
        end = i * 100 + 99
        res_json = get_page(problem_id[start:end])
        df = transform(res_json)
        if i == 0:
            df.to_csv(csv_file, encoding='utf-8', index=False)
        else:
            df.to_csv(csv_file, mode='a', header=False, index=False)
@task
def upload_local_file_to_s3():
    local_file_path = os.path.join(os.getcwd(),'data','problem_list.csv')
    s3_bucket_name = 'soomers-backjoon'
    s3_file_key = 'engine/problems.csv'
    s3_hook = S3Hook(aws_conn_id='soomers_aws_conn_id')
    s3_hook.load_file(
        filename=local_file_path,
        key=s3_file_key,
        bucket_name=s3_bucket_name,
        replace=True
    )

with DAG(
    dag_id = 'problem_csv_to_s3',
    start_date = datetime(2023,6,23), # 날짜가 미래인 경우 실행이 안됨
    schedule = '0 0 * * *',  # 적당히 조절
    catchup = False,
    max_active_runs = 1,
    default_args = {
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag :
    paging = get_page_all()
    uploading = upload_local_file_to_s3()
    paging >> uploading

