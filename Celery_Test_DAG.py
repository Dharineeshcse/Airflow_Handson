from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import time
import socket

def test_task():
    print("====== Celery Task Started ====== ")
    print(f"Worker hostname: {socket.gethostname()}")
    print("Sleeping for 5 seconds...")
    time.sleep(5)
    print("====== Celery Task Completed ======")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="celery_executor_test",
    default_args=default_args,
    start_date=datetime(2023, 1, 1),
    schedule=None ,
    catchup=False,
) as dag:

    run_test = PythonOperator(
        task_id="run_celery_task",
        python_callable=test_task,
    )
