from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from PIL import Image
import numpy as np
import os


# ==========================
# CONFIG
# ==========================
IMAGE_PATH = "/data/images/google-logo.jpg"        # From PVC
OUTPUT_DIR = "/data/output"                   # Will be inside PVC
os.makedirs(OUTPUT_DIR, exist_ok=True)


# ==========================
# TASK FUNCTIONS
# ==========================

def extract_image(**kwargs):
    """Load image and push pixel array to XCom."""
    img = Image.open(IMAGE_PATH)
    arr = np.array(img)

    print("Extracted image shape:", arr.shape)
    kwargs["ti"].xcom_push(key="raw_image", value=arr.tolist())


def convert_to_grayscale(**kwargs):
    """Convert RGB image to grayscale."""
    raw = kwargs["ti"].xcom_pull(key="raw_image")
    arr = np.array(raw)

    gray = np.mean(arr, axis=2)   # simple grayscale conversion

    print("Converted to grayscale:", gray.shape)
    kwargs["ti"].xcom_push(key="gray_image", value=gray.tolist())


def save_output(**kwargs):
    """Save grayscale image back to PVC."""
    gray = np.array(kwargs["ti"].xcom_pull(key="gray_image"))

    out_path = f"{OUTPUT_DIR}/output_grayscale.png"
    img = Image.fromarray(gray.astype("uint8"))
    img.save(out_path)

    print("Saved grayscale output to:", out_path)


# ==========================
# DAG DEFINITION
# ==========================

with DAG(
    dag_id="ml_image_grayscale_etl",
    description="Extract image → Convert to grayscale → Save output",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
    default_args={"owner": "airflow"},
) as dag:

    extract = PythonOperator(
        task_id="extract_image",
        python_callable=extract_image,
        executor_config={
            "KubernetesExecutor": {
                "image": "dharineesh22/ml-image:1.0"   
            }
        }
    )

    grayscale = PythonOperator(
        task_id="convert_to_grayscale",
        python_callable=convert_to_grayscale,
        executor_config={
            "KubernetesExecutor": {
                "image": "dharineesh22/ml-image:1.0"
            }
        }
    )

    save = PythonOperator(
        task_id="save_output_image",
        python_callable=save_output,
        executor_config={
            "KubernetesExecutor": {
                "image": "dharineesh22/ml-image:1.0"
            }
        }
    )

    extract >> grayscale >> save
