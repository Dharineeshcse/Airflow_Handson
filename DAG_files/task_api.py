import json
import pendulum
from airflow.sdk import dag, task


@dag(
    dag_id="taskflow_api_dag",
    schedule=None,
    start_date=pendulum.datetime(2025, 11, 9, tz="UTC"),
    tags=["task_api"],
)
def example_taskflow_api():

    @task(queue="ml_tasks")
    def extract() -> dict[str, int]:
        data_string = '{"land1": 80, "land2": 75, "land3": 19}'
        land_data_dict = json.loads(data_string)
        return land_data_dict

    @task(queue="ml_tasks")
    def transform(land_data_dict: dict[str, int]) -> dict[str, int]:
        total_value = 0
        multi_value = 1
        for value in land_data_dict.values():
            total_value += value
            multi_value *= value
        return {"total_value": total_value, "multi_value": multi_value}

    @task(queue="ml_tasks")
    def load_total(total_value: int) -> None:
        print(f"Total value is: {total_value}")

    @task(queue="ml_tasks")
    def load_multiple(multiple_value: int) -> None:
        print(f"Multiple value is: {multiple_value}")

    # Dependencies
    land_data = extract()
    summary = transform(land_data)
    load_total(summary["total_value"])
    load_multiple(summary["multi_value"])


dag = example_taskflow_api()
