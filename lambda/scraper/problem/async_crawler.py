import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
from copy import deepcopy
import aiohttp
import asyncio


class Scraper:    
    def __init__(self, flag:str) -> None:
        self.flag = flag
        self.problems = list()
        self.users = list()
        self.workbooks = list()
        self.workbook_rank = 1
        self.user_agent_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            ]   
    
    
    async def get_object_thread(self, base_url:str, start:int, end:int) -> None:
        async with aiohttp.ClientSession() as session:  # 시스템의 DNS resolver를 사용하는 세션을 만듭니다.
            tasks = []
            for p_index in range(start, end+1):
                
                if self.flag == 'problem':
                    task = asyncio.ensure_future(self.get_problem_parser(base_url=base_url, p_index=p_index, session=session))
                elif self.flag == 'user':
                    task = asyncio.ensure_future(self.get_user_parser(base_url=base_url, p_index=p_index, session=session))
                elif self.flag == 'workbook':
                    await asyncio.sleep(random.randint(1,2)) # 스크래퍼 휴식
                    task = asyncio.ensure_future(self.get_workbook_parser(base_url=base_url, p_index=p_index, session=session))
                tasks.append(task)
            await asyncio.gather(*tasks)
    
    
    async def get_problem_parser(self, base_url:str, p_index:int, session) -> None:
        url = base_url + str(p_index)
        headers = {'User-Agent': random.choice(self.user_agent_list)}
        
        async with session.get(url, headers=headers) as response:
            html = await response.text()
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
                problem["problem_id"] = cols[0]
                problem["problem_title"] = cols[1]
                problem["information"] = cols[2]
                problem["problem_answer_num"] = cols[3]
                problem["problem_submit_num"] = cols[4]
                problem["problem_answer_rate"] = float(cols[5].strip('%'))
                
                self.problems.append(problem)
                # print(problem)
    
    
    async def get_user_parser(self, base_url:str, p_index:int, session) -> None:
        url = base_url + str(p_index)
        headers = {'User-Agent': random.choice(self.user_agent_list)}
        
        async with session.get(url, headers=headers) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')
            table = soup.find(class_='table table-striped table-bordered no-mathjax')
            # print(table)
            rows = table.find_all('tr')
            # table header 제거
            # 한번에 100개씩 총 1000번 가져옴 -> 35분 이상 걸림
            rows = rows[1:]
            for row in rows:
                cols = row.find_all('td')
                # 문제, 문제 제목, 정보, 맞힌 사람, 제출, 정답 비율
                cols = [ele.text.strip() for ele in cols]

                user = dict()
                user["user_rank"] = cols[0]
                user["user_id"] = cols[1]
                user["status_message"] = cols[2]
                user["user_answer_num"] = cols[3]
                user["user_submit_num"] = cols[4]
                user["user_answer_rate"] = float(cols[5].strip('%'))
                # user_detail = Scraper.user_api(user_id=user["id"])
                # user.update(user_detail)
                self.users.append(user)
                print(user)
    
    
    async def get_workbook_parser(self, base_url:str, p_index:int, session) -> None:
        url = f'{base_url}/top/{str(p_index)}'
        headers = {'User-Agent': random.choice(self.user_agent_list)}
        
        async with session.get(url, headers=headers) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            table = soup.find(class_='table table-striped table-bordered')
            rows = table.find_all('tr')
            # table header 제거
            rows = rows[1:]
            for row in rows:
                cols = row.find_all('td')            
                cols = [ele.text.strip() for ele in cols]

                workbook = dict()
                workbook["workbook_rank"] = self.workbook_rank
                workbook["workbook_id"] = cols[0]
                workbook["made_person"] = cols[1]
                workbook["workbook_title"] = cols[2]
                problem_list = await self.get_workbook_problems_parser(workbook=workbook ,base_url = base_url, session=session)
                
                self.workbooks.extend(problem_list)
                self.workbook_rank += 1

    
    async def get_workbook_problems_parser(self, workbook:dict, base_url:str, session) -> list:
        
        workbook_id = workbook['workbook_id']
        url = f'{base_url}/view/{str(workbook_id)}'
        headers = {'User-Agent': random.choice(self.user_agent_list)}

        async with session.get(url, headers=headers) as response:
            html = await response.text()
            soup = BeautifulSoup(html, 'html.parser')

            table = soup.find(class_='table table-striped table-bordered')
            rows = table.find_all('tr')
            # table header 제거
            rows = rows[1:] 
            problem_list = list()
            for row in rows:
                cols = row.find_all('td')            
                cols = [ele.text.strip() for ele in cols]
                problem_dict = deepcopy(workbook)
                problem_dict["problem_id"] = cols[0]
                problem_dict["problem_title"] = cols[1]
                # problem_dict["problem_answer_num"] = cols[3]
                # problem_dict["problem_submit_num"] = cols[4]
                # problem_dict["problem_answer_rate"] = cols[5]
                problem_list.append(problem_dict)
            
        return problem_list
    
    
    def save_to_csv(self, objects: list, file_name: str) -> None:
        """Save a list of objects to a CSV file.

        Args:
            objects (list): A list of objects to save to a CSV file.
            file_name (str): The name of the CSV file to save the objects to.
        """
        df = pd.DataFrame(objects)
        if self.flag == 'problem':
            df['problem_id'] = df['problem_id'].astype(int)
            df = df.sort_values('problem_id')  # Sort by 'id' column in ascending order
            df.to_csv(file_name, index=False, encoding='utf-8-sig')
            
        elif self.flag == 'user':
            df['user_rank'] = df['user_rank'].astype(int)
            df = df.sort_values('user_rank')  # Sort by 'id' column in ascending order
            df.to_csv(file_name, index=False, encoding='utf-8-sig')
            
        elif self.flag == 'workbook':
            # Ensure 'id' column is int for correct sorting
            df['workbook_id'] = df['workbook_id'].astype(int)
            # df = df.sort_values('workbook_rank')
            df.to_csv(file_name, index=False, encoding='utf-8-sig')
        
        else:
            raise Exception('invalid flag')


'''
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
'''

