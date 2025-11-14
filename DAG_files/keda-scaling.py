from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# This DAG will generate a large number of scheduled + queued tasks
# so that KEDA can scale the scheduler up.

with DAG(
    dag_id="keda_scheduler_load_test_dag",
    start_date=datetime(2025, 1, 1),
    schedule="@once",  # run only once when triggered
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")

    # Create 200 lightweight tasks
    tasks = [EmptyOperator(task_id=f"task_{i}") for i in range(1, 201)]

    end = EmptyOperator(task_id="end")

    start >> tasks >> end
