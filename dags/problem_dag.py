from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import concurrent.futures
# The ProblemScraper class should be imported or defined here
from plugins import crawler
import os


def scrape_problems(**context):
    base_url = context["params"]["url"]
    start = context["params"]["start"]
    end = context["params"]["end"]
    scraper = crawler.Scraper(flag='problem')
    scraper.get_object_thread(base_url, start, end)
    # print(soup)
    # not find_all
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     start_indexes = list(range(1, 271, 54))  
    #     end_indexes = start_indexes[1:] + [271]
    #     executor.map(scraper.get_problems_thread, base_url, start_indexes, end_indexes)
        
    context['ti'].xcom_push(key='problem_scraper', value=scraper.problems)

def save_to_csv(**context):
    scraper = crawler.Scraper(flag='problem')
    scraper_objects = context['ti'].xcom_pull(key='problem_scraper')
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "problems.csv")
    scraper.save_to_csv(objects = scraper_objects, file_name=file_path)
    print(file_path)


default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

with DAG(
    'problem_scraper',
    default_args=default_args,
    description='A user scraper DAG',
    schedule_interval='@once',
) as dag:

    scrape_problems_task = PythonOperator(
        task_id='scrape_problems',
        python_callable=scrape_problems,
        provide_context=True,
        params = {
                'url': "https://www.acmicpc.net/problemset/",
                'start' : 1,
                'end' : 270
            },
        dag=dag,
    )

    save_to_csv_task = PythonOperator(
        task_id='save_to_csv',
        python_callable=save_to_csv,
        provide_context=True,
        dag=dag,
    )

    # Define dependencies
    scrape_problems_task >> save_to_csv_task
    
