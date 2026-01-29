from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import time


def print_task(task_number):
    time.sleep(5)
    print(f"simple task {task_number}")


with DAG(
    dag_id="Print_Numbers_Kubernetes_Executor",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    default_args={
        "executor": "KubernetesExecutor"  
    },
) as dag:

    for i in range(1, 30):
        PythonOperator(
            task_id=f"task_{i}",
            python_callable=print_task,
            op_args=[i],
        )
