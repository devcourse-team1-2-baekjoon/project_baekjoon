from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import concurrent.futures
# The ProblemScraper class should be imported or defined here
from plugins import problem_crawler
import os

default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
}

dag = DAG(
    'problem_scraper',
    default_args=default_args,
    description='A problem scraper DAG',
    schedule_interval='@once',
)

def scrape_problems(**context):
    base_url = context["params"]["url"]
    start = context["params"]["start"]
    end = context["params"]["end"]
    scraper = problem_crawler.ProblemScraper()

    scraper.get_problems_thread(base_url, start, end)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     start_indexes = list(range(1, 271, 54))  
    #     end_indexes = start_indexes[1:] + [271]
    #     executor.map(scraper.get_problems_thread, base_url, start_indexes, end_indexes)
        
    context['ti'].xcom_push(key='scraper', value=scraper.problems)

def save_to_csv(**context):
    scraper = problem_crawler.ProblemScraper()
    scraper_problems = context['ti'].xcom_pull(key='scraper')
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "problems.csv")
    scraper.save_to_csv(scraper_problems, file_path)
    print(file_path)

scrape_problems = PythonOperator(
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

save_to_csv = PythonOperator(
    task_id='save_to_csv',
    python_callable=save_to_csv,
    provide_context=True,
    dag=dag,
)

# Define dependencies
scrape_problems >> save_to_csv