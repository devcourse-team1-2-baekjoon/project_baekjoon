from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import concurrent.futures

from plugins import crawler
from airflow.decorators import task # decorator 임포트

import time
import os
import pandas as pd


@task
def scrape_workbook_user(url, start, end, flag) -> list:
    
    print('start scrape_workbook_user task')
    start_time = time.time()
    scraper = crawler.Scraper(flag=flag)
    scraper.get_object_thread(base_url=url, start=start, end=end)
    end_time = time.time()
    print(f'scrape_workbook_user {start_time-end_time} seconds')
    return scraper.workbooks


@task
def save_to_csv(scraper_object:list, flag:str) -> None:
    
    print('start save_to_csv task')
    scraper = crawler.Scraper(flag=flag)
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, f"workbooks.csv")
    scraper.save_to_csv(objects=scraper_object, file_name=file_path)
    print('end save_to_csv task')
    
    
default_args = {
    'owner': 'airflow',
    'catchup': False,
    'start_date': datetime(2023, 5, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=3),
    'max_active_tasks' : 5
}

with DAG(
    'workbook_scraper',
    default_args=default_args,
    description='A workbook scraper DAG',
    schedule_interval='@once',
) as dag:
    
    url = "https://www.acmicpc.net/workbook"
    flag = 'workbook'


    start, end = 1, 20
    scraper_task = scrape_workbook_user(url, start, end, flag)
    save_task = save_to_csv(scraper_object=scraper_task, flag=flag)




# @task
# def merge_csvs(num_csvs: int) -> None:
#     output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/workbook")
#     dfs = []

#     # read all csv files and append to a list
#     for i in range(num_csvs):
#         file_path = os.path.join(output_folder, f"workbooks_{i+1}.csv")
#         dfs.append(pd.read_csv(file_path))

#     # concatenate all dataframes in the list
#     df_combined = pd.concat(dfs, ignore_index=True)
#     df_combined = df_combined.sort_values('workbook_rank')

#     # write the combined dataframe to a new csv file
#     combined_file_path = os.path.join(output_folder, "combined_workbooks.csv")
#     df_combined.to_csv(combined_file_path, index=False)
    
    
    # start_indexes = list(range(1, 20, 4)) 
    # end_indexes = start_indexes[1:] + [20]
    # task_chains = []
    
    # for index, start_end in enumerate(zip(start_indexes, end_indexes)):
    #     start, end = start_end
    #     scraper_task = scrape_workbook_user(url, start, end, flag)
    #     save_task = save_to_csv(scraper_object=scraper_task, flag=flag, index=index)
        
    #     task_chain = scraper_task >> save_task
    #     task_chains.append(task_chain)


    # merge_task = merge_csvs(len(start_indexes))
    
    # task_chains >> merge_task
    
    
    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     start_indexes = list(range(1, 271, 54))  
    #     end_indexes = start_indexes[1:] + [271]
    #     executor.map(scraper.get_problems_thread, base_url, start_indexes, end_indexes)
    
    
 
    # {'workbook_rank': 200, 'workbook_id': '7273', 'made_person': 'tony9402', 'workbook_title': '최단거리 (수정 : 2021-05-06)', 'problem_id': '18243', 'problem_title': 'Small World Network', 'answer_num': '420', 'submit_num': '1083', 'answer_rate': '46.615%'}, 