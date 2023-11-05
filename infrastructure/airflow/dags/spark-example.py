from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator

# from airflow.providers.trino.operators.trino import TrinoOperator


from airflow.providers.apache.spark.operators.spark_sql import SparkSqlOperator


def my_python_function():
    print("Hello World")


dag = DAG(
    dag_id='spark-example',
    tags=['osds'],
    schedule='@daily',
    max_active_runs=1,
    start_date=datetime(2017, 1, 1),
    end_date=datetime(2017, 1, 10)
)

run_task = PythonOperator(
    dag=dag,
    task_id='run_python_script',
    python_callable=my_python_function
)

spark_sql_job = SparkSqlOperator(
    dag=dag,
    sql="SELECT COUNT(1) as cnt FROM temp_table",
    master="local",
    task_id="spark_sql_job"
)
