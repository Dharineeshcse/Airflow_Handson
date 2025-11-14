from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def simple_task(task_number):
    print(f"Running task number {task_number}")
    time.sleep(20)  # keep it running for longer


with DAG(
    dag_id="keda_scaling_test_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    for i in range(200):
        PythonOperator(
            task_id=f"task_{i}",
            python_callable=simple_task,
            op_kwargs={"task_number": i},
            pool="test_pool",      # forces QUEUED state
        )
