from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from BSW_TFT.routines.summoner_by_name import get_summoners_by_name 
import json


def read_from_mongo(**context):
    
    mongo = MongoHook('mongo')
    mongo_pipeline = [
    {
        '$group': {
            '_id': {
                'tier': '$tier', 
                'division': '$division'
            }, 
            'count': {
                '$first': {
                    'entries': '$entries.summonerName', 
                    'region': '$region'
                }
            }
        }
    }, {
        '$unwind': {
            'path': '$count.entries'
        }
    }, {
        '$project': {
            '_id': 0, 
            'name': '$count.entries', 
            'region': '$count.region'
        }
    }
]

    all_summoners = mongo.aggregate(mongo_collection='league', aggregate_query=mongo_pipeline)
    summoners_names_list = []
    for i in all_summoners:
        summoners_names_list.append(i)
    
    t1.xcom_push(key="extract_summoner", value=summoners_names_list, context=context)
    
def extract(**context):
    
    data_from_mongo = t2.xcom_pull(key="extract_summoner", context=context)
    summoners_obj_list = get_summoners_by_name(data_from_mongo)
    t2.xcom_push(key="extract_summoner", value=summoners_obj_list, context=context)    
    
def save_to_mongo(**context):
    
    summoners_obj_list = t3.xcom_pull(key="extract_summoner", context=context)
    mongo = MongoHook('mongo')
    mongo.insert_many(mongo_collection='summoner', docs=summoners_obj_list)

with DAG(
    dag_id='extract_summoner',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    start_date=datetime(2022, 7, 23),
    catchup=False,
    tags=['extract', 'TFT'],
) as dag:

    t1 = PythonOperator(
        task_id='mongo_read',
        provide_context=True,
        python_callable=read_from_mongo
    )

    t2 = PythonOperator(
        task_id='extract',
        provide_context=True,
        python_callable=extract
    )
    
    t3 = PythonOperator(
        task_id='mongo_save',
        python_callable=save_to_mongo
    )

t1 >> t2 >> t3