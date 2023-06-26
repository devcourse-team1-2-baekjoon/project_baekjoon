import requests

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
            print(user_detail)
        except:
            print('no user detail')
            pass
    
    return user_detail

user_api('michro')