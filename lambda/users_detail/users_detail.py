from datetime import datetime, timedelta
from dotenv import load_dotenv

import os
import pandas as pd

import aiohttp
from fake_useragent import UserAgent
import asyncio
import time
import boto3

ua = UserAgent()

async def fetch_page(session, url, page, user_agent, semaphore):
    async with semaphore:
        headers = {"User-agent": user_agent}
        querystring = {"query": '', "page": page}
        async with session.get(url, headers=headers, params=querystring) as response:
            if response.status == 200:
                print(f"Processing page {page}")
                return await response.json()
            else:
                return None

async def collect_data(max_requests: int) -> list:
    url = "https://solved.ac/api/v3/search/user"
    semaphore = asyncio.Semaphore(max_requests)

    async with aiohttp.ClientSession() as session:
        data_list = []

        try:
            tasks = []
            for page in range(1, 1300):
                tasks.append(fetch_page(session, url, page, ua.random, semaphore))
            pages_data = await asyncio.gather(*tasks)

            for data in pages_data:
                if data is None:
                    continue
                items = data['items']

                for item in items:
                    if item['tier'] <= 6:
                        print("Tier 6 reached. Stopping data collection.")
                        return data_list

                    filtered_item = {
                        'user_id': item['handle'],
                        'user_answer_num': item['solvedCount'],
                        'user_tier': item['tier'],
                        'user_rating': item['rating'],
                        'user_ratingByProblemsSum': item['ratingByProblemsSum'],
                        'user_ratingByClass': item['ratingByClass'],
                        'user_ratingBySolvedCount': item['ratingBySolvedCount'],
                        'user_ratingByVoteCount': item['ratingByVoteCount'],
                        'user_class': item['class'],
                        'user_maxStreak': item['maxStreak'],
                        'user_joinedAt': item['joinedAt'],
                        'user_rank': item['rank']
                    }
                    data_list.append(filtered_item)
                    await asyncio.sleep(0.1)

        except Exception as e:
            print(f"오류 발생: {e}")

        return data_list

async def collect_data_and_save_to_csv(max_requests: int) -> None:
    data = await collect_data(max_requests)

    if data:
        df = pd.DataFrame(data)
        csv_file = '/tmp/users_detail.csv'
        df.to_csv(csv_file, index=False, encoding='utf-8')
        print("Data collection and saving completed successfully.")
        await upload_to_s3(csv_file)

def upload_to_s3(file_path: str) -> None:
    load_dotenv()
    bucket_name = os.environ.get('bucket_name')
    access_key = os.environ.get('access_key')
    secret_key = os.environ.get('secret_key')
    region_name = "ap-northeast-2"

    s3 = boto3.client('s3', aws_access_key_id=access_key, aws_secret_access_key=secret_key, region_name=region_name)
    s3.upload_file(file_path, bucket_name, "users_detail.csv")
    print("CSV file uploaded to S3 successfully.")

async def main(max_requests: int) -> None:
    start_time = time.time()
    print("Data collection started.")
    await collect_data_and_save_to_csv(max_requests)
    end_time = time.time()
    print(f"실행 시간: {end_time - start_time}")


def lambda_handler(event, context):
    max_requests = 50
    asyncio.run(main(max_requests))
    upload_to_s3('/tmp/users_detail.csv')
    return {    
        'statusCode': 200,
        'body': 'users_detail.csv uploaded to S3 successfully.'
    }


