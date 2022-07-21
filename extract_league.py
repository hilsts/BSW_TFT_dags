from BSW_TFT.routines.full_region_league import extract_league
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta
from airflow.providers.mongo.hooks.mongo import MongoHook

def extract(**context):
    data = extract_league()
    t1.xcom_push(key="extract_league", value=data, context=context)

def save_to_mongo(**context):

    data = t2.xcom_pull(key="extract_league", context=context)
    print(data)
    mongo = MongoHook('mongo')
    for i in data:
        mongo.insert_many(mongo_collection='league', docs=i)

with DAG(
    dag_id='extract_league',
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 6),
    catchup=False,
    tags=['extract', 'TFT'],
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