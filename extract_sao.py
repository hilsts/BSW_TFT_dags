from BSW_TFT.routines.matches_by_name import matches_by_name
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook
from airflow.models import Variable
import json

SAO_PLAYERS = json.loads(Variable.get("SAO"))

def extract(**context):
    
    all_sao_matches = []
    for sao_player in SAO_PLAYERS:
        print(sao_player)
        match_list = matches_by_name(summoner_name=sao_player)
        all_sao_matches.append(match_list)
    t1.xcom_push(key="extract_sao", value=all_sao_matches, context=context)
    
    


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