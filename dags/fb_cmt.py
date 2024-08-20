from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import json
from pymongo import MongoClient
import pandas as pd
import requests
import json
import time
import os
import json
from pymongo import MongoClient
from airflow.utils.task_group import TaskGroup
from pymongo.errors import ConnectionFailure, InvalidDocument

def get_all_comments(sess, post_id, ACCESS_TOKEN, COOKIE):
    comments = []
    next_url = f'https://graph.facebook.com/{post_id}/comments?access_token={ACCESS_TOKEN}&fields=message,from,created_time,like_count&limit=100'
    while next_url:
        try:    
            response = sess.get(next_url, headers={'cookie': COOKIE})
            data = response.json()
            comments.extend(data['data'])
            next_url = data.get('paging', {}).get('next')
            # time.sleep(30)
        except Exception as e:
            print("Error fetching comments:", e)
            return None  # Trả về None nếu có lỗi
    return comments

def get_data(sess, url, number, data_path, LIMIT, ACCESS_TOKEN, COOKIE):
    print("Data saved to folder data")
    for i in range(1):
        try:
            print(f'Getting {LIMIT} posts from {url}')
            response = sess.get(url, headers={'cookie': COOKIE})
            response = json.loads(response.text)
            for post in response['data']:
                post_id = post['id']
                comments = get_all_comments(sess, post_id, ACCESS_TOKEN, COOKIE)
                if comments is None:  # Kiểm tra nếu get_all_comments trả về None
                    raise Exception("Error occurred in get_all_comments")
                post['comments'] = comments
            with open(f"{data_path}/data_{number}.json", "w") as f:
                json.dump(response, f, ensure_ascii=False, indent=4)
            url = response['paging']['next']
            
            last_number = number + 1
            last_info = {
                    'next_url': url,
                    'last_number': last_number,
                    'begin_numer': number,
                }
            with open(f"{data_path}/last_info.json", "w") as f:
                json.dump(last_info, f, ensure_ascii=False, indent=4)
            print(f"End: Sleep 30 seconds\n")
            time.sleep(30)
        except Exception as e:
            print("Error occurred in get_data:", e)
            break

def connect_db(db_name, collection_name):
    try:
        client = MongoClient('mongodb://host.docker.internal:27017')
        db = client[db_name]
        # Kiểm tra và tạo collection nếu chưa tồn tại
        if collection_name not in db.list_collection_names():
            db.create_collection(collection_name)
            print(f"Collection '{collection_name}' created in database '{db_name}'")
        else:
            print(f"Collection '{collection_name}' already exists in database '{db_name}'")
        
        print("Connected to MongoDB")
        collection = db[collection_name]
        return collection
    except ConnectionFailure as e:
        print("Could not connect to MongoDB:", e)
        return None
    

def crawling_data(BRAND):
    LIMIT = 10 # https://developers.facebook.com/docs/graph-api/overview/rate-limiting
    ACCESS_TOKEN = 'EAAGNO4a7r2wBOZCVlh9zy2smvZCUJlXAWa34j7ZATtVLkyFGNoeHuOaWQvPJpaZAH7MzcGXEagh4oFaZBnHgNXamnqkaICZBZA2mFZCotm9SuMkK4dGhw9da4Ku6oJm3SGGQBCgdzndnEOsYAdailt1mjOloAKbY29qsqb76qJrYAUFYdszfUgu5UW48YBCogKS18wZDZD'
    COOKIE = 'sb=ziNXZp1TRb8gP05r-gRoBsLu; datr=ziNXZpT8cqxfl4fd9Np2bwd1; ps_n=1; ps_l=1; c_user=61560558020522; dpr=1.5; xs=45%3A-QXB6kzVFWhlIg%3A2%3A1717216681%3A-1%3A-1%3A%3AAcVjAhFokxRnszQpeLwf5QrsKshSd050bKZAlo78fvA; presence=C%7B%22t3%22%3A%5B%5D%2C%22utc3%22%3A1718696882281%2C%22v%22%3A1%7D; usida=eyJ2ZXIiOjEsImlkIjoiQXNmOW4wYnplM3pjciIsInRpbWUiOjE3MTg2OTY4OTF9; fr=19fGw7PEKBLSlKZoo.AWUZ-sSiOh5qg9aKAOmpgfaGIi4.BmbW5b..AAA.0.0.BmcTvD.AWVUrO-Hp8g; wd=1280x593'
    data_path = f"/tmp/data/{BRAND}"
    os.makedirs(data_path,exist_ok=True)
    sess = requests.Session()
    if os.path.exists(f"{data_path}/last_info.json"):
        print('found next url info')
        with open(f"{data_path}/last_info.json", "r") as f:
            last_info=json.load(f)
            url=last_info['next_url']
            number=last_info['last_number']
    else:
        print('not found next url info')
        url = f'https://graph.facebook.com/{BRAND}/feed?limit={LIMIT}&access_token={ACCESS_TOKEN}&fields=message,shares,likes.summary(true),comments'
        number = 0
    get_data(sess, url, number, data_path, LIMIT, ACCESS_TOKEN, COOKIE)


def upload_to_db(BRAND):
    data_path = f"/tmp/data/{BRAND}"
    collection=connect_db('fb',BRAND)
    if collection is None:
        return
    
    if os.path.exists(f"{data_path}/last_info.json"):
        print('found next url info')
        with open(f"{data_path}/last_info.json", "r") as f:
            last_info=json.load(f)
            last_number=last_info['last_number']
            begin_numer=last_info['begin_numer']
    else:
        last_number=8
        begin_numer=0

    list_file = sorted(os.listdir(data_path))
    list_file = [os.path.join(data_path, file) for file in list_file if file.startswith('data_') and int(file.split('_')[1].split('.')[0]) >= begin_numer and int(file.split('_')[1].split('.')[0]) < last_number]
    for comment_file in list_file:
        try:
            with open(comment_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
                documents = []
                for ann in data['data']:
                    for comment in ann['comments']:
                        doc = {
                            "ID_Post": ann['id'],
                            "ID_Comment": comment["id"],
                            "ID_User": comment["from"]["id"],
                            "Brand": BRAND,
                            "Comment": comment["message"],
                            "Comment_Like": comment["like_count"],
                            "Date": comment["created_time"]
                        }
                        documents.append(doc)

                # Insert các document vào MongoDB
                if documents:
                    collection.insert_many(documents)
                    print(f"Inserted {len(documents)} documents into MongoDB")

        except (IOError, json.JSONDecodeError) as e:
            print(f"Error reading file {comment_file}: {e}")
        except InvalidDocument as e:
            print(f"Error in document structure: {e}")
        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    print("Upload completed!")


default_args = {
    'owner': 'quan',
    'start_date': datetime.now(),
    'email': ['21521333@gm.uit.edu.vn'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Initialize the DAG
dag = DAG(
    'craw_fb_data',
    default_args=default_args,
    description='crawling fb comment fast food brand',
    schedule_interval='@daily',
)

def crawling_data_KFC():
    crawling_data('KFC')

def upload_to_db_KFC():
    upload_to_db('KFC')

def crawling_data_McDonalds():
    crawling_data('McDonalds')

def upload_to_db_McDonalds():
    upload_to_db('McDonalds')

def crawling_data_DominosPizza():
    crawling_data('DominosPizza')

def upload_to_db_DominosPizza():
    upload_to_db('DominosPizza')

def crawling_data_Subway():
    crawling_data('Subway')

def upload_to_db_Subway():
    upload_to_db('Subway')



crawling_data_KFC = PythonOperator(
    task_id='crawling_data_KFC',
    python_callable=crawling_data_KFC,
    dag=dag,
)

upload_to_db_KFC = PythonOperator(
    task_id='upload_to_db_KFC',
    python_callable=upload_to_db_KFC,
    dag=dag,
)

crawling_data_McDonalds = PythonOperator(
    task_id='crawling_data_McDonalds',
    python_callable=crawling_data_McDonalds,
    dag=dag,
)

upload_to_db_McDonalds = PythonOperator(
    task_id='upload_to_db_McDonalds',
    python_callable=upload_to_db_McDonalds,
    dag=dag,
)

crawling_data_DominosPizza = PythonOperator(
    task_id='crawling_data_DominosPizza',
    python_callable=crawling_data_DominosPizza,
    dag=dag,
)

upload_to_db_DominosPizza = PythonOperator(
    task_id='upload_to_db_DominosPizza',
    python_callable=upload_to_db_DominosPizza,
    dag=dag,
)

crawling_data_Subway = PythonOperator(
    task_id='crawling_data_Subway',
    python_callable=crawling_data_Subway,
    dag=dag,
)

upload_to_db_Subway = PythonOperator(
    task_id='upload_to_db_Subway',
    python_callable=upload_to_db_Subway,
    dag=dag,
)

crawling_data_KFC >> upload_to_db_KFC >> crawling_data_McDonalds >> upload_to_db_McDonalds >> crawling_data_DominosPizza >> upload_to_db_DominosPizza >> crawling_data_Subway >> upload_to_db_Subway