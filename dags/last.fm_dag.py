from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import datetime

from update import runUpdate
from init import runInit

# from update import run_update

initial_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 9, 19),
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

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 9, 19),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=720),
}

dag = DAG(
    'last.fm_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval=timedelta(days=1),
)

update_etl = PythonOperator(
    task_id='update_etl',
    python_callable=runUpdate,
    dag=dag,
)

update_etl