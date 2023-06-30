from datetime import datetime, timedelta
from dotenv import load_dotenv

import time
import json
from async_crawler import Scraper
import boto3

import asyncio
import os

def scrape_problems(url:str, start:int, end:int) -> list:
    base_url = url
    start = start
    end = end
    
    start_time = time.time()
    scraper = Scraper(flag='problem')
    print(f"start problem crawling from {start} to {end}")
    
    # loop = asyncio.get_event_loop()
    # loop.run_until_complete(scraper.get_object_thread(base_url, start, end))
    # loop.close()
    asyncio.run(scraper.get_object_thread(base_url, start, end))
    
    end_time = time.time()
    print(f"Elapsed Time: {start_time - end_time}")
    
    return scraper.problems


def save_to_csv(scraper_objects:list) -> None:
    scraper = Scraper(flag='problem')
    
    # writeable directory
    output_folder = "/tmp"

    file_path = os.path.join(output_folder, "problems.csv")
    scraper.save_to_csv(objects = scraper_objects, file_name=file_path)
    print(file_path)
    return file_path


def upload_to_s3(file_path: str, bucket_name: str, access_key: str, secret_key: str, region_name: str) -> None:
    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
    
    s3.upload_file(file_path, bucket_name, "baekjoon/problems.csv")


def lambda_handler(event, context):
    url = "https://www.acmicpc.net/problemset/"
    start = 1
    end = 270
    
    load_dotenv()
    
    bucket_name = os.environ.get('bucket_name')
    access_key = os.environ.get('access_key')
    secret_key = os.environ.get('secret_key')
    region_name = "ap-northeast-2"
    

    scraper_objects = scrape_problems(url, start, end)
    file_path = save_to_csv(scraper_objects)
    upload_to_s3(file_path, bucket_name, access_key, secret_key, region_name)
    
    print("problem is scraped successfully!")

    return {
        'statusCode': 200,
        'body': 'problem is scraped successfully!'
    }


