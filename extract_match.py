from BSW_TFT.routines.matches_by_name import matches_by_name
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
import json

MATCH_CONFIG = json.loads(Variable.get("match_config"))

def extract(**context):

    mongo = MongoHook('mongo')
    
    league_pipeline = [
        {
            '$match': {
                'tier': {
                    '$in': [
                        'CHALLENGER', 'GRANDMASTER', 'MASTER', 'DIAMOND'
                    ]
                }
            }
        }, {
            '$project': {
                '_id': 0, 
                'entries': 1
            }
        }, {
            '$unwind': {
                'path': '$entries'
            }
        }, {
            '$project': {
                'summonerName': '$entries.summonerName'
            }
        }, {
            '$lookup': {
                'from': 'summoner', 
                'localField': 'summonerName', 
                'foreignField': 'name', 
                'as': 'string'
            }
        }, {
            '$match': {
                'string.puuid': {
                    '$exists': 1
                }
            }
        }, {
            '$unwind': {
                'path': '$string'
            }
        }, {
            '$project': {
                'puuid': '$string.puuid'
            }
        }
    ]
    
    filtered_summoner_puuids = mongo.aggregate(mongo_collection='league', aggregate_query=league_pipeline)
    
    avoided_matches = mongo.find(mongo_collection='match', query={}, projection={"match_id" : "$metadata.match_id"})
    
    
    
    for i in filtered_summoner_puuids:
        
        i["puuid"]
        
            
        
    
    
    
    


def save_to_mongo(**context):

    data = t2.xcom_pull(key="extract_sao", context=context)
    print(data)
    mongo = MongoHook('mongo')
    
    
    for i in data:
        mongo.insert_many(mongo_collection='match', docs=i)

with DAG(
    dag_id='extract_sao',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 6),
    catchup=False,
    tags=['extract', 'TFT', 'SAO'],
) as dag:

    t1 = PythonOperator(
        task_id='api_get',
        provide_context=True,
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id='mongo_save',
        python_callable=save_to_mongo
    )

t1 >> t2