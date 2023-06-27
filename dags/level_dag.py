from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import concurrent.futures

from airflow.decorators import task # decorator 임포트

import os
import csv

@task
def save_to_csv() -> None:
    
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "level.csv")
    
    csvfile = open(file_path, "w", newline="")
    csvwriter = csv.writer(csvfile)

    header = ['level', 'rank']
    csvwriter.writerow(header)

    data = ['Unrated',
            'Bronze V',
            'Bronze IV',
            'Bronze III',
            'Bronze II',
            'Bronze I',
            'Silver V',
            'Silver IV',
            'Silver III',
            'Silver II',
            'Silver I',
            'Gold V',
            'Gold IV',
            'Gold III',
            'Gold II',
            'Gold I',
            'Platinum V',
            'Platinum IV',
            'Platinum III',
            'Platinum II',
            'Platinum I',
            'Diamond V',
            'Diamond IV',
            'Diamond III',
            'Diamond II',
            'Diamond I',
            'Ruby V',
            'Ruby IV',
            'Ruby III',
            'Ruby II',
            'Ruby I']
    
    for i in range(len(data)):
        row = [i, data[i]]
        csvwriter.writerow(row)

    csvfile.close()

    print(file_path)


default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'save_level_csv',
    default_args=default_args,
    description='save level info to csv DAG',
    schedule_interval='@once',
) as dag:
    
    save_to_csv()
    # Define dependencies
