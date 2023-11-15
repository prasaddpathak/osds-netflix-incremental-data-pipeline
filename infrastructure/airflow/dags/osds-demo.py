from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.copy_source_data import copy_source_data
from scripts.movie_analytics import process_movie_analytics
from scripts.user_analytics import process_user_analytics
from scripts.infra_analytics import process_infra_analytics

default_args = {
    'owner': 'prasaddpathak',
    'depends_on_past': True,
    'email': ['prasaddpathak@gmail.com'],
    # If a task fails, retry it once after waiting
    # at least 5 minutes
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    dag_id='osds-demo',
    tags=['osds'],
    schedule='@daily',
    default_args=default_args,
    max_active_runs=1,
    start_date=datetime(2017, 1, 1),
    end_date=datetime(2017, 4, 1)
)

copy_source_data = PythonOperator(
    dag=dag,
    task_id='copy_source_data',
    python_callable=copy_source_data,
    op_args=['{{ds}}']
)

process_movie_analytics = PythonOperator(
    dag=dag,
    task_id='process_movie_analytics',
    python_callable=process_movie_analytics,
    op_args=['{{ds}}']
)

process_user_analytics = PythonOperator(
    dag=dag,
    task_id='process_user_analytics',
    python_callable=process_user_analytics,
    op_args=['{{ds}}']
)

process_infra_analytics = PythonOperator(
    dag=dag,
    task_id='process_infra_analytics',
    python_callable=process_infra_analytics,
    op_args=['{{ds}}']
)

copy_source_data >> [process_movie_analytics, process_user_analytics, process_infra_analytics]
