import requests
from bs4 import BeautifulSoup
import pandas as pd
import os 
import random
import concurrent.futures

class ProblemScraper:
    def __init__(self):
        self.problems = list()
        
    def get_problems_thread(self, base_url:str, start:int, end:int):
        for p_index in range(start, end+1):
            self.get_movies(base_url, p_index)
    
    def get_movies(self, base_url:str, p_index:int):
        url = base_url + str(p_index)
        
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

            problem = dict()
            problem["id"] = cols[0]
            problem["title"] = cols[1]
            problem["information"] = cols[2]
            problem["answer_num"] = cols[3]
            problem["submit_num"] = cols[4]
            problem["answer_rate"] = float(cols[5].strip('%'))
            
            self.problems.append(problem)
            print(problem)
        
    @staticmethod
    def save_to_csv(problems: list, file_name: str):
        # problems -> id asc 별로 sort한 후 저장
        df = pd.DataFrame(problems)
        df['id'] = df['id'].astype(int)  # Ensure 'id' column is int for correct sorting
        df = df.sort_values('id')  # Sort by 'id' column in ascending order
        df.to_csv(file_name, index=False, encoding='utf-8-sig')




    

