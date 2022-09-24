from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

from init import runInit

initial_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 9, 19), # change to today
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

init = DAG(
    'init',
    default_args=initial_args,
    description='Initialization DAG',
    schedule_interval='@once',
)

init_etl = PythonOperator(
    task_id='init_etl',
    python_callable=runInit,
    dag=init,
)

init_etl