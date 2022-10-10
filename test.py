from colorama import Cursor
from airflow.providers.mysql.hooks.mysql import MySqlHook

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

def test_conn():
    print('TESTING CONNECTION !!!!!!')
    mariadbhook = MySqlHook(mysql_conn_id='mariadb', schema='')
    
    mongo_pipeline = [
        {
            '$unwind': {
                'path': '$metadata.participants'
            }
        }, {
            '$lookup': {
                'from': 'summoner', 
                'localField': 'metadata.participants', 
                'foreignField': 'puuid', 
                'as': 'test'
            }
        }
    ]

    conn = mariadbhook.get_conn()
    cur = conn.cursor()
    
    cur.execute("""SELECT User, Host  FROM mysql.user;""")

    data = cur.fetchall()
    print(data)
    
with DAG(
    dag_id='test',
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
         task_id='teste',
         python_callable=test_conn,
         dag=dag
     )
     
t1