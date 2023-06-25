import requests
from bs4 import BeautifulSoup
import pandas as pd
import os 
import random
from datetime import datetime
import aiohttp
import time
import asyncio

class ProblemScraper:
    def __init__(self, url):
        self.base_url = url
        self.problems = []
    
    def get_movies(self, p_index):
        url = self.base_url + str(p_index)
        
        user_agents_list = [
        'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
        ]   

        response = requests.get(url, headers={'User-Agent': random.choice(user_agents_list)})
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')

        # print(soup)
        # not find_all
        table = soup.find(class_='table table-striped table-bordered clickable-table')
        # print(table)
        rows = table.find_all('tr')
        # table header 제거
        rows = rows[1:]
        for row in rows:
            cols = row.find_all('td')
            # 문제, 문제 제목, 정보, 맞힌 사람, 제출, 정답 비율
            cols = [ele.text.strip() for ele in cols]
            print(cols)

            problem = dict()
            problem["id"] = cols[0]
            problem["title"] = cols[1]
            problem["information"] = cols[2]
            problem["answer_num"] = cols[3]
            problem["submit_num"] = cols[4]
            problem["answer_rate"] = cols[5]
            
            self.problems.append(problem)
            print(problem)
        

    def save_to_csv(self, file_name):
        # problems -> id asc 별로 sort한 후 저장
        df = pd.DataFrame(self.problems)
        df.to_csv(file_name, index=False, encoding='utf-8-sig')



if __name__ == "__main__":
    base_url = "https://www.acmicpc.net/problemset/"
    scraper = ProblemScraper(base_url)
    # p_index = 1
    # scraper.get_movies(p_index)
    
    for p_index in range(1, 271):
        scraper.get_movies(p_index)
    
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)
    
    file_path = os.path.join(output_folder, "problems.csv")
    scraper.save_to_csv(file_path)
    print(file_path)
    

