from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

from scripts.copy_source_data import copy_source_data


dag = DAG(
    dag_id='osds-demo',
    tags=['osds'],
    schedule='@daily',
    max_active_runs=1,
    start_date=datetime(2017, 1, 1),
    end_date=datetime(2017, 1, 5)
)

run_task = PythonOperator(
    dag=dag,
    task_id='copy_source_data',
    python_callable=copy_source_data,
    op_args=['{{ds}}']
)
