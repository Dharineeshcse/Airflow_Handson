import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.empty import EmptyOperator

sample_dag = DAG(
    dag_id = "sample_dag",
    start_date = datetime.datetime(2025, 11, 11),
    schedule = None,
    catchup = False,
)


t1 = EmptyOperator(task_id = "task1", dag = sample_dag)

t2 = EmptyOperator(task_id = "task2", dag = sample_dag)


t1 >> t2