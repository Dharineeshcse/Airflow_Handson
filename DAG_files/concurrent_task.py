from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_task(task_number):
    print(f"simple task {task_number}")


with DAG(
    dag_id="large_100_task_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,  # manual trigger
    catchup=False,
    concurrency=200,
    max_active_tasks=200,
    max_active_runs=10,
    default_args={"owner": "airflow"},
) as dag:

    tasks = []

    for i in range(1, 101):
        task = PythonOperator(
            task_id=f"task_{i}",
            python_callable=print_task,
            op_args=[i],
        )
        tasks.append(task)

