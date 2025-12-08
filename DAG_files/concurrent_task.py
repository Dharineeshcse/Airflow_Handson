from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time

time.sleep(1)

def print_task(task_number):
    print(f"simple task {task_number}")


with DAG(
    dag_id="large_100_task_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,  # manual trigger
    catchup=False,
) as dag:

    tasks = []

    for i in range(1, 502):
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=print_task,
            op_args=[i],
            queue="ml_tasks",
        )
        tasks.append(task)

