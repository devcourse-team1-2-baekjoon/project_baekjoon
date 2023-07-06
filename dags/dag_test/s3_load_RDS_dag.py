from airflow.decorators import task
from airflow import DAG
from datetime import datetime

import boto3
import psycopg2

# AWS 인증 정보
aws_access_key_id = 'aws_access_key_id'
aws_secret_access_key = 'aws_secret_access_key'

# PostgreSQL 연결 정보
db_host = 'db_host'
db_name = 'db_name'
db_user = 'db_user'
db_password = 'db_password'

default_args = {
    'start_date': datetime(2023, 7, 6),
    'schedule_interval': None,
    'catchup': False,
}

@task
def get_s3_files(bucket_name, directory_name=''):
    s3 = boto3.client('s3', aws_access_key_id=aws_access_key_id, aws_secret_access_key=aws_secret_access_key)
    objects = s3.list_objects(Bucket=bucket_name, Prefix=directory_name)
    files = []
    if 'Contents' in objects:
        for obj in objects['Contents']:
            file_key = obj['Key']
            file_name = file_key.split('/')[-1]
            if file_name:
                files.append(file_name)
    return files

@task
def load_csv_to_db(file_name):
    table_name = 'raw_data.users'
    s3_bucket_name = 'baekjoon-project-pipeline'
    directory_name = 'directory_name'
    s3_file_path = f's3://{s3_bucket_name}/{directory_name}/{file_name}'

    copy_query = f"""
        COPY {table_name}
        FROM '{s3_file_path}'
        ACCESS_KEY_ID '{aws_access_key_id}'
        SECRET_ACCESS_KEY '{aws_secret_access_key}'
        IGNOREHEADER 1
        DELIMITER ','
        CSV;
    """

    conn = psycopg2.connect(host=db_host, dbname=db_name, user=db_user, password=db_password)
    cur = conn.cursor()
    cur.execute(copy_query)
    conn.commit()
    cur.close()
    conn.close()

with DAG(
    dag_id='load_csv_to_db',
    default_args=default_args,
    description='Load csv data from S3 to RDS',
    schedule_interval=None,
    catchup=False,
) as dag:

    list_of_files = get_s3_files('your_bucket_name', 'directory_name')

    for file in list_of_files:
        load_csv = load_csv_to_db(file)

    get_s3_files() >> load_csv_to_db
