from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os
from PIL import Image
import numpy as np



IMAGE_PATH = "/opt/airflow/dags/repo/google-logo.jpg" 
OUTPUT_DIR = "/opt/airflow/dags/output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def extract_image(**kwargs):
    """Load image and push pixel array to XCom."""
    img = Image.open(IMAGE_PATH)
    arr = np.array(img)
    kwargs['ti'].xcom_push(key='raw_image', value=arr.tolist())


def convert_to_grayscale(**kwargs):
    """Convert image to grayscale."""
    arr = np.array(kwargs['ti'].xcom_pull(key='raw_image'))
    gray = np.mean(arr, axis=2)  # simple grayscale
    kwargs['ti'].xcom_push(key='gray_image', value=gray.tolist())


def ml_inference(**kwargs):
    """Perform simple ML logic (example: mean pixel intensity)."""
    gray = np.array(kwargs['ti'].xcom_pull(key='gray_image'))
    score = float(np.mean(gray))  # Example model output
    kwargs['ti'].xcom_push(key='score', value=score)


def store_final_value(**kwargs):
    """Save final ML output."""
    score = kwargs['ti'].xcom_pull(key='score')
    output_file = f"{OUTPUT_DIR}/final_output.txt"
    with open(output_file, "w") as f:
        f.write(f"Final ML Score: {score}\n")
    print(f"Saved result to {output_file}")


# -------------------------------
# DAG DEFINITION
# -------------------------------
with DAG(
    dag_id="ml_image_etl_pipeline",
    description="ETL + ML on image data",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    default_args={"owner": "airflow"},
) as dag:

    extract = PythonOperator(
        task_id="extract_image",
        python_callable=extract_image,
    )

    grayscale = PythonOperator(
        task_id="convert_to_grayscale",
        python_callable=convert_to_grayscale,
    )

    inference = PythonOperator(
        task_id="ml_inference",
        python_callable=ml_inference,
    )

    store = PythonOperator(
        task_id="store_final_value",
        python_callable=store_final_value,
    )

    extract >> grayscale >> inference >> store
