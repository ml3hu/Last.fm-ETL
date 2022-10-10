from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python import ShortCircuitOperator
import datetime
from airflow.utils.trigger_rule import TriggerRule

from init_status import check_initialized
from extract_init import initial_extract
from transform import transform
from extract_update import update_extract
from load import load
from init_db import initialize_database
from empty_stage import empty
from export import to_csv


initial_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.datetime(2022, 10, 9), # change to today
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    'Last.fm_ETL',
    default_args=initial_args,
    description='Last.fm ETL Pipeline',
    schedule_interval=datetime.timedelta(days=1),
) as dag:
    check_init = BranchPythonOperator(
    task_id='check_initialized',
    python_callable=check_initialized,
    dag=dag,
    )

    extract_init = ShortCircuitOperator(
    task_id='initial_extract',
    python_callable=initial_extract,
    dag=dag,
    )

    extract_update = ShortCircuitOperator(
    task_id='update_extract',
    python_callable=update_extract,
    dag=dag,
    )

    init_db = PythonOperator(
    task_id='initialize_database',
    python_callable=initialize_database,
    dag=dag,
    )

    empty_stage = PythonOperator(
    task_id='empty_stage',
    python_callable=empty,
    dag=dag,
    )

    task_transform = PythonOperator(
    task_id='transform',
    python_callable=transform,
    trigger_rule=TriggerRule.NONE_FAILED,
    dag=dag,
    )

    task_load = PythonOperator(
    task_id='load',
    python_callable=load,
    dag=dag,
    )

    export = PythonOperator(
    task_id='export',
    python_callable=to_csv,
    dag=dag,
    )


    check_init >> [extract_init, extract_update]
    extract_init >> init_db >> task_transform
    extract_update >> empty_stage >> task_transform
    task_transform >> task_load >> export


