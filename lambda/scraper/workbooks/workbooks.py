from datetime import datetime, timedelta
from dotenv import load_dotenv

import json
from async_crawler import Scraper
import boto3

import asyncio
import os
import time

def scrape_workbook_user(url:str, start:int, end:int, flag:str) -> list:
    base_url = url
    start = start
    end = end
    print('start scrape_workbook_user task')
    start_time = time.time()
    
    scraper = Scraper(flag=flag)
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(scraper.get_object_thread(base_url = base_url, start = start, end = end))
    # loop.close()     
    asyncio.run(scraper.get_object_thread(base_url, start, end))
    
    end_time = time.time()
    print(f'scrape_workbook_user {start_time-end_time} seconds')
    return scraper.workbooks


def save_to_csv(scraper_object:list, flag:str) -> None:
    scraper = Scraper(flag=flag)
    
    # writeable directory
    output_folder = "/tmp"

    file_path = os.path.join(output_folder, "workbooks.csv")
    scraper.save_to_csv(objects = scraper_object, file_name=file_path)
    print(file_path)
    return file_path


def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
    
    s3.upload_file(file_path, bucket_name, "baekjoon/workbooks.csv")


def lambda_handler(event, context):
    
    url = "https://www.acmicpc.net/workbook"    
    start, end = 1, 20
        
    flag = 'workbook'
    load_dotenv()
    
    bucket_name = os.environ.get('bucket_name')
    access_key = os.environ.get('access_key')
    secret_key = os.environ.get('secret_key')
    region_name = "ap-northeast-2"
    
    scraper_objects = scrape_workbook_user(url, start, end, flag)
    file_path = save_to_csv(scraper_object=scraper_objects, flag=flag)
    upload_to_s3(file_path, bucket_name, access_key, secret_key, region_name)
    
    print('workbooks is scraped successfully!')

    return {
        'statusCode': 200,
        'body': 'workbooks is scraped successfully!'
    }


