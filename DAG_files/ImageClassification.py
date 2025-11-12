from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime

IMAGE_PATH = "/data/images/sample.jpg"
OUTPUT_DIR = "/data/output"


def extract_image(**kwargs):
    from PIL import Image
    import numpy as np
    import os

    os.makedirs("/data/output", exist_ok=True)

    img = Image.open(IMAGE_PATH)
    arr = np.array(img)
    kwargs['ti'].xcom_push(key='raw_image', value=arr.tolist())


def convert_to_grayscale(**kwargs):
    import numpy as np

    arr = np.array(kwargs['ti'].xcom_pull(key='raw_image'))
    gray = np.mean(arr, axis=2)
    kwargs['ti'].xcom_push(key='gray_image', value=gray.tolist())


def save_output(**kwargs):
    from PIL import Image
    import numpy as np
    import os

    os.makedirs("/data/output", exist_ok=True)

    gray = np.array(kwargs['ti'].xcom_pull(key='gray_image'))
    img = Image.fromarray(gray.astype("uint8"))
    img.save(f"{OUTPUT_DIR}/output_grayscale.png")


with DAG(
    dag_id="ml_image_grayscale_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_image,
        executor_config={
            "KubernetesExecutor": {
                "pod_override": {
                    "spec": {
                        "containers": [
                            {
                                "name": "base",
                                "image": "dharineesh22/ml-image:1.0"
                            }
                        ]
                    }
                }
            }
        },
    )

    grayscale = PythonOperator(
        task_id="grayscale",
        python_callable=convert_to_grayscale,
        executor_config={
            "KubernetesExecutor": {
                "pod_override": {
                    "spec": {
                        "containers": [
                            {
                                "name": "base",
                                "image": "dharineesh22/ml-image:1.0"
                            }
                        ]
                    }
                }
            }
        },
    )

    save = PythonOperator(
        task_id="save",
        python_callable=save_output,
        executor_config={
            "KubernetesExecutor": {
                "pod_override": {
                    "spec": {
                        "containers": [
                            {
                                "name": "base",
                                "image": "dharineesh22/ml-image:1.0"
                            }
                        ]
                    }
                }
            }
        },
    )

    extract >> grayscale >> save
