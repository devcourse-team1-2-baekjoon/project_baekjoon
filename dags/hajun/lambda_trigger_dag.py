from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.amazon.aws.operators.aws_lambda import AwsLambdaInvokeFunctionOperator

from airflow.providers.amazon.aws.hooks.lambda_function import LambdaHook

import json


default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
}


with DAG(
    dag_id='lambda_trigger_dag',
    default_args=default_args,
    description='A lambda trigger DAG',
    schedule_interval='@once',
) as dag:

    
    # invoke_lambda_function = AwsLambdaInvokeFunctionOperator(
    #     task_id="invoke_lambda_function",
    #     function_name= "crawler_problem",
    #     payload=json.dumps({"event": "trigger_dag"}),
    #     log_type="Tail",
    #     aws_conn_id = 'lambda_aws_conn_id'
    # )
    
    invoke_lambda_function = AwsLambdaInvokeFunctionOperator(
        task_id="invoke_lambda_function",
        function_name= "crawler_problem",
        payload=json.dumps({"event": "trigger_dag"}),
        log_type="Tail",
        aws_conn_id = 'lambda_aws_conn_id'
    )

    invoke_lambda_function