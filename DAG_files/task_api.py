import json

import pendulum
from airflow.sdk import dag, task


# 1. Define a dag using the @dag decorator
@dag(
    schedule=None,
    start_date=pendulum.datetime(2022, 5, 1, tz="UTC"),
    tags=["example"],
)
def example_taskflow_api():
    # 2. Define tasks using the @task decorator
    @task()
    def extract() -> dict[str, int]:
        data_string = '{"land1": 80, "land2": 75, "land3": 19}'

        land_data_dict = json.loads(data_string)

        return land_data_dict

    @task()
    def transform(land_data_dict: dict[str, int]) -> dict[str, int]:
        total_value = 0
        multi_value = 1
        for value in land_data_dict.values():
            total_value += value
            multi_value *= value

        return {"total_value": total_value, "multi_value": multi_value}

    @task()
    def load_total(total_value: int) -> None:
        print(f"Total value is: {total_value}")

    @task()
    def load_multiple(multiple_value: int) -> None:
        print(f"Multiple value is: {multiple_value}")

    # 3. Define data (task) dependencies
    land_data = extract()
    order_summary = transform(land_data)
    load_total(order_summary["total_value"])
    load_multiple(order_summary["multi_value"])


dag = example_taskflow_api()