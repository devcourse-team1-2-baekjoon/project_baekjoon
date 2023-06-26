import requests
from bs4 import BeautifulSoup
import pandas as pd
import os 
import random
import concurrent.futures

class Scraper:    
    def __init__(self, flag:str) -> None:
        self.flag = flag
        self.problems = list()
        self.users = list()
        self.solvedac_users = list()
        self.user_agent_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            ]   
    
    
    def get_object_thread(self, base_url:str, start:int, end:int) -> None:
        for p_index in range(start, end+1):
            if self.flag == 'problem':
                self.get_problem_parser(base_url=base_url, p_index = p_index)
            elif self.flag == 'user':
                self.get_user_parser(base_url=base_url, p_index = p_index)
            else:
                self.get_solvedac_user_parser(base_url=base_url, p_index = p_index)
    
    
    def get_problem_parser(self, base_url:str, p_index:int) -> None:
        url = base_url + str(p_index)
        
        user_agents_list = self.user_agent_list

        response = requests.get(url, headers={'User-Agent': random.choice(user_agents_list)})
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
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
    
    
    def get_user_parser(self, base_url:str, p_index:int) -> None:
        url = base_url + str(p_index)
        user_agents_list = self.user_agent_list
        response = requests.get(url, headers={'User-Agent': random.choice(user_agents_list)})
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        table = soup.find(class_='table table-striped table-bordered no-mathjax')
        # print(table)
        rows = table.find_all('tr')
        # table header 제거
        rows = rows[1:]
        for row in rows:
            cols = row.find_all('td')
            # 문제, 문제 제목, 정보, 맞힌 사람, 제출, 정답 비율
            cols = [ele.text.strip() for ele in cols]

            user = dict()
            user["rank"] = cols[0]
            user["id"] = cols[1]
            user["status_message"] = cols[2]
            user["answer_num"] = cols[3]
            user["submit_num"] = cols[4]
            user["answer_rate"] = float(cols[5].strip('%'))
            # user_detail = Scraper.user_api(user_id=user["id"])
            # user.update(user_detail)
            self.users.append(user)
            print(user)
    
    
    def get_solvedac_user_parser(self, base_url:str, p_index:int) -> None:
        url = f'{base_url}?page={str(p_index)}'
        user_agents_list = self.user_agent_list
        response = requests.get(url, headers={'User-Agent': random.choice(user_agents_list)})
        html = response.text
        soup = BeautifulSoup(html, 'html.parser')
        # table = soup.find(class_='css-a651il')
        # print(table)
        rows = soup.find_all(class_='css-1ojb0xa')
        # table header 제거
        rows = rows[1:]
        for row in rows:
            cols = row.find_all('td')            
            cols = [ele.text.strip() for ele in cols]

            user = dict()
            user["rank"] = cols[0]
            user["id"] = cols[1]
            user["ac_rating"] = cols[2]
            user["class"] = cols[3]
            user["answer_num"] = cols[4]
  
            user_detail = Scraper.user_api(user_id=user["id"])
            user.update(user_detail)
            self.solvedac_users.append(user)
            print(user)   
    
    
    @staticmethod
    def user_api(user_id:str) -> dict:
     
        url = f"https://solved.ac/api/v3/user/show?handle={user_id}"
        headers = {"Accept": "application/json"}
        response = requests.get(url, headers=headers)
        # print(response.json())
        user_detail = dict()
        if response.status_code == 200:
            try:
                json_object = response.json()
                user_detail['bio'] = json_object['bio']
                user_detail['profileImageUrl'] = json_object['profileImageUrl']
                user_detail['solvedCount'] = json_object['solvedCount']
                user_detail['voteCount'] = json_object['voteCount']
                user_detail['tier'] = json_object['tier']
                user_detail['rating'] = json_object['rating']
                user_detail['class'] = json_object['class']
                user_detail['classDecoration'] = json_object['classDecoration']
                user_detail['maxStreak'] = json_object['maxStreak']
                # print(user_detail)
            except:
                print('no user detail')
                pass
            
        elif response.status_code == 429:
            print('Too many response')
            pass
    
        return user_detail
        
        
    
    def save_to_csv(self, objects: list, file_name: str) -> None:
        # problems -> id asc 별로 sort한 후 저장
        df = pd.DataFrame(objects)
        if self.flag == 'problem':
            df['id'] = df['id'].astype(int)  # Ensure 'id' column is int for correct sorting
            df = df.sort_values('id')  # Sort by 'id' column in ascending order
            df.to_csv(file_name, index=False, encoding='utf-8-sig')
        elif self.flag == 'user':
            df['rank'] = df['rank'].astype(int)  # Ensure 'id' column is int for correct sorting
            df = df.sort_values('rank')  # Sort by 'id' column in ascending order
            df.to_csv(file_name, index=False, encoding='utf-8-sig')

        else:
            # Ensure 'id' column is int for correct sorting
            df['rank'] = df['rank'].str.replace(',', '').astype(int)
            df = df.sort_values('rank')  # Sort by 'id' column in ascending order
            df.to_csv(file_name, index=False, encoding='utf-8-sig')


    


