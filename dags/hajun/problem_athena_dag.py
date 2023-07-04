from datetime import datetime
from airflow import DAG

from plugins import athena_s3
import time

default_args = {
    'owner': 'airflow',
    'catchup': False,  # 과거의 DAG 실행 여부
    'start_date': datetime(2023, 5, 1),  # DAG의 시작 날짜 및 시간
}

with DAG(
    'problem_athena_query',
    default_args=default_args,
    schedule_interval='@once'  # DAG의 실행 주기 설정 (매일 00:00에 실행)
) as dag:
    
    athena_query = """
    SELECT * FROM problems ORDER BY problem_id DESC LIMIT 20;
    """
    
    crawler = athena_s3.GlueTriggerCrawlerOperator(
        aws_conn_id='hajun_aws_conn_id',
        task_id='run_s3_crawler',
        crawler_name= 'baekjoon_problem_crawler'
    )
    
    run_query = athena_s3.XComEnabledAWSAthenaOperator(
        task_id='run_query',
        query=athena_query,
        output_location='s3://airflow-bucket-hajun/athena_problem/',
        database='test',
        aws_conn_id='hajun_aws_conn_id',
    )
    
    move_results = athena_s3.S3FileRenameOperator(
        task_id='move_results',
        source_bucket='airflow-bucket-hajun',
        source_key='athena_problem/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
        destination_bucket='airflow-bucket-hajun',
        destination_key='athena_problem/problem.csv',
        aws_conn_id='hajun_aws_conn_id',
        dag=dag,
    )
    
    crawler >> run_query >> move_results
    
    