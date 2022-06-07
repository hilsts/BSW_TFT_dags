from BSW_TFT.routines.full_region_league import extract_league
from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta


with DAG(
    'extract_league',
    # These args will get passed on to each operator
    # You can override them on a per-task basis during operator initialization
    default_args={
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        # 'queue': 'bash_queue',
        # 'pool': 'backfill',
        # 'priority_weight': 10,
        # 'end_date': datetime(2016, 1, 1),
        # 'wait_for_downstream': False,
        # 'sla': timedelta(hours=2),
        # 'execution_timeout': timedelta(seconds=300),
        # 'on_failure_callback': some_function,
        # 'on_success_callback': some_other_function,
        # 'on_retry_callback': another_function,
        # 'sla_miss_callback': yet_another_function,
        # 'trigger_rule': 'all_success'
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 6),
    catchup=False,
    tags=['extract', 'TFT'],
) as dag:

    t1 = PythonOperator(
        task_id='api_get',
        python_callable=extract_league
    )

t1