import uuid
import json
import time
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2023, 1, 1),
}

#get data from the api
def get_data():
    import requests
    return requests.get("https://randomuser.me/api/").json()['results'][0]

#take the important detail and format the data in required form
def format_data(res):
    location = res['location']
    return {
        'id': str(uuid.uuid4()),  # ✅ FIX 1: JSON serializable
        'first_name': res['name']['first'],
        'last_name': res['name']['last'],
        'gender': res['gender'],
        'address': f"{location['street']['number']} {location['street']['name']}, "
                   f"{location['city']}, {location['state']}, {location['country']}",
        'post_code': location['postcode'],
        'email': res['email'],
        'username': res['login']['username'],
        'dob': res['dob']['date'],
        'registered_date': res['registered']['date'],
        'phone': res['phone'],
        'picture': res['picture']['medium']
    }

#function tell how to produce and duration of dag to run
def stream_data():
    from kafka import KafkaProducer

    producer = KafkaProducer(
        bootstrap_servers=['broker:29092'],
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        acks='all'
    )

    end_time = datetime.now() + timedelta(seconds=30)

    while datetime.now() < end_time: #run upto the end time (duration)
        try:
            res = format_data(get_data())
            producer.send('users_created', res)
            time.sleep(1)
        except Exception as e:
            logging.error(f"Kafka produce error: {e}")
            raise

    producer.flush()
    producer.close()

#dags for airflow to orchestrate..
with DAG(
    dag_id='user_automation',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
