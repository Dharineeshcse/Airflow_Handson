from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="print_hi_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,   # Run manually
    catchup=False,
) as dag:

    print_hi = BashOperator(
        task_id="sample_dag_task_2",
        bash_command="echo created a ci/cd pipeline and made the job to run on my linux server"
    )
