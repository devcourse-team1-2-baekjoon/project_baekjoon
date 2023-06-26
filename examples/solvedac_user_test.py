import requests
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.utils import ChromeType
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait

import pandas as pd
import os 
import random
import concurrent.futures
import time

class Scraper:    
    def __init__(self) -> None:
        self.solvedac_users = list()
        self.driver = None
        self.user_agent_list = [
            'Mozilla/5.0 (iPad; CPU OS 12_2 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Mobile/15E148',
            'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.83 Safari/537.36',
            'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36'
            ]   
        

    def driver_get(self) -> object:
        
        executable_path = "./chromedriver"
        if os.path.exists(executable_path):
            driver_path = executable_path
        else:
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        
        print("chromedriver path: ", driver_path)
        
        assert os.path.exists(driver_path), "Chromedriver executable not found"
        
        if os.path.exists(driver_path):
            os.chmod(driver_path, 0o755)  # chromedriver 권한 설정
        
        chrome_options = Options()
        options = [
            # "--headless=new",
            "--no-sandbox"
            "--disable-gpu",
            "--start-maximized",
            # "--window-size=1980,1030",
            # "--window-size=1920,1200",
            "--ignore-certificate-errors",
            "--disable-infobars",
            "--disable-extensions",
            "--disable-dev-shm-usage"
        ]

        if options:
            for option in options:
                chrome_options.add_argument(option)

        # service = ChromeService(executable_path=driver_path)
        driver = webdriver.Chrome(driver_path, options=chrome_options)
        self.driver = driver
        
        return driver
     
    
    def get_object_thread(self, base_url:str, start:int, end:int) -> None:
        for p_index in range(start, end+1):
            a = random.randint(1, 2)
            time.sleep(a)
            self.get_solvedac_user_parser(base_url=base_url, p_index = p_index)
    
    
    def get_solvedac_user_parser(self, base_url:str, p_index:int) -> None:
        # driver = self.driver_get()
        
        url = f'{base_url}?page={str(p_index)}'
        user_agents_list = self.user_agent_list
        # driver = self.driver
        # driver.get(url)
        # driver.implicitly_wait(3)
        
        # page_source = driver.page_source
        
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
            print('Too many request')
            pass
    
        return user_detail
    
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
            print('Too many request')
            pass
    
        return user_detail
    
    

    
    def save_to_csv(self, objects: list, file_name: str) -> None:
        # problems -> id asc 별로 sort한 후 저장
        df = pd.DataFrame(objects)
        
        df['rank'] = df['rank'].str.replace(',', '').astype(int)
        df = df.sort_values('rank')  # Sort by 'id' column in ascending order
        df.to_csv(file_name, index=False, encoding='utf-8-sig')


def scrape_solvedac_user(url, start, end) -> list:

    scraper = Scraper()
    scraper.get_object_thread(base_url=url, start=start, end=end)

    # with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
    #     start_indexes = list(range(1, 271, 54))  
    #     end_indexes = start_indexes[1:] + [271]
    #     executor.map(scraper.get_problems_thread, base_url, start_indexes, end_indexes)
    # context['ti'].xcom_push(key='user_scraper', value=scraper.users)
    return scraper.solvedac_users

def save_to_csv(scraper_objects:list) -> None:
    scraper = Scraper()
    output_folder = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    file_path = os.path.join(output_folder, "solvedac_users.csv")
    scraper.save_to_csv(objects=scraper_objects, file_name=file_path)
    print(file_path)

if __name__ =='__main__':
    url = "https://solved.ac/ko/ranking/tier"
    start = 1
    end = 1500

    scraper_objects = scrape_solvedac_user(url, start, end)
    save_to_csv(scraper_objects)