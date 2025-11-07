from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Simple DAG that runs tasks as K8s pods via KubernetesExecutor
with DAG(
    dag_id="kubernetes_executor_test",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    # This command will run inside a NEW POD created by KubernetesExecutor
    task1 = BashOperator(
        task_id="run_in_k8s_pod",
        bash_command='echo "Hello from KubernetesExecutor! My pod name is $HOSTNAME"; sleep 10'
    )

    task2 = BashOperator(
        task_id="second_k8s_task",
        bash_command='echo "This is another pod running..."'
    )

    task1 >> task2
