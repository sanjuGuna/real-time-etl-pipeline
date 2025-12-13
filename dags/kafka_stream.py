from datetime import datetime
from airflow import DAG
import airflow.operators.python
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
    'owner':'airflow',
    'start_date':datetime(2025, 12, 15 ,10, 00),
}

def get_data():# get data from the api and returns it
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0] #taking only the required data
    #print(json.dumps(res, indent=3))
    return res

def format_data(res):   #extracting and formating the data before into kafka queue
    data = {}
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    location= res['location']
    data['address'] = f"{location['street']['number']} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data
def stream_data():
    import json
    from kafka import KafkaProducer

    res = get_data()
    res = format_data(res)

    producer = KafkaProducer(
        bootstrap_servers=['localhost:9092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )

    producer.send('user_created', res)
    producer.flush()  #ensuring all the messages are sent and acknowledged before exting
    producer.close()




# with DAG('user_automation',
#          default_args=default_args,
#          schedule_interval='@daily',
#          catchup=False) as dag:
#     streaming_task = PythonOperator(
#         task_id='streaming_data_from_api',
#         python_callable=stream_data
#     )

stream_data()