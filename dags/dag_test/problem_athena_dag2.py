from datetime import datetime
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

default_args = {
    'owner': 'airflow',
    'catchup': False,  # 과거의 DAG 실행 여부
    'start_date': datetime(2023, 5, 1),  # DAG의 시작 날짜 및 시간
}


dag = DAG(
    'problem_athena_query_2',
    default_args=default_args,
    schedule_interval='@once'  # DAG의 실행 주기 설정 (매일 00:00에 실행)
)

# athena_query = """
#     UNLOAD (SELECT * FROM problem ORDER BY problem_id DESC LIMIT 20)
#     TO 's3://airflow-bucket-hajun/athena_output/'
#     WITH (format = 'parquet')
#     """

athena_query = """
SELECT * FROM problems ORDER BY problem_id DESC LIMIT 20;
"""

athena_task = AthenaOperator(
    task_id='run_problem_athena_query',
    query=athena_query,
    database='new_test',
    output_location='s3://airflow-bucket-hajun/athena_output/',
    aws_conn_id='hajun_aws_conn_id',
    dag=dag
)

athena_task
    


'''

    # athena_query = """
    # CREATE TABLE athena_problem_test
    # AS
    # SELECT * FROM problem ORDER BY problem_id DESC LIMIT 20;
    # """

    # athena_query = """
    # UNLOAD (SELECT * FROM problem ORDER BY problem_id DESC LIMIT 20)
    # TO 's3://airflow-bucket-hajun/athena_output/'
    # WITH (format = 'csv')
    # """

    # athena_task = AthenaOperator(
    #     task_id='run_problem_athena_query',
    #     query=athena_query,
    #     database='test',
    #     output_location='s3://airflow-bucket-hajun/athena_output/',
    #     aws_conn_id='hajun_aws_conn_id',
    #     dag=dag
    # )
    
    
        # move_results = S3FileTransformOperator(
    #     task_id='move_results',
    #     source_s3_key='s3://airflow-bucket-hajun/athena_output/{{ task_instance.xcom_pull(task_ids="run_query") }}.csv',
    #     dest_s3_key='s3://airflow-bucket-hajun/athena_output/problem.csv',
    #     transform_script='/bin/cp',
    # )
'''