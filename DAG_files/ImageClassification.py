from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kubernetes.client import models as k8s

# IMAGE_PATH = "/data/images/google-logo.jpg"
# OUTPUT_DIR = "/data/output"


# volume = k8s.V1Volume(
#     name="data-volume",
#     persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-ml-pvc")
# )

# volume_mount = k8s.V1VolumeMount(
#     name="data-volume",
#     mount_path="/data"
# )


# pod_override = k8s.V1Pod(
#     spec=k8s.V1PodSpec(
#         containers=[
#             k8s.V1Container(
#                 name="base",
#                 image="dharineesh22/ml-image:1.1",
#                 volume_mounts=[volume_mount]
#             )
#         ],
#         volumes=[volume]
#     )
# )


# def extract_image(**kwargs):
#     from PIL import Image
#     import numpy as np
#     import os

#     os.makedirs("/data/output", exist_ok=True, mode=777)

#     img = Image.open(IMAGE_PATH)
#     arr = np.array(img)
#     kwargs['ti'].xcom_push(key='raw_image', value=arr.tolist())


# def convert_to_grayscale(**kwargs):
#     import numpy as np

#     arr = np.array(kwargs['ti'].xcom_pull(key='raw_image'))
#     gray = np.mean(arr, axis=2)
#     kwargs['ti'].xcom_push(key='gray_image', value=gray.tolist())


# def save_output(**kwargs):
#     from PIL import Image
#     import numpy as np
#     import os

#     os.makedirs("/data/output", exist_ok=True, mode=777)

#     gray = np.array(kwargs['ti'].xcom_pull(key='gray_image'))
#     img = Image.fromarray(gray.astype("uint8"))
#     img.save(f"{OUTPUT_DIR}/output_grayscale.png")


# with DAG(
#     dag_id="ml_image_grayscale_etl",
#     start_date=datetime(2025, 1, 1),
#     schedule=None,
#     catchup=False,
# ) as dag:

#     extract = PythonOperator(
#         task_id="extract",
#         python_callable=extract_image,
#         executor_config={"pod_override": pod_override},


#     )

#     grayscale = PythonOperator(
#         task_id="grayscale",
#         python_callable=convert_to_grayscale,
#         executor_config={"pod_override": pod_override},

#     )

#     save = PythonOperator(
#         task_id="save",
#         python_callable=save_output,
#         executor_config={"pod_override": pod_override},

#     )

#     extract >> grayscale >> save



IMAGE_PATH = "/data/images/google-logo.jpg"
OUTPUT_DIR = "/data/output"
TEMP_DIR = "/data/temp"  # Temporary directory for intermediate files

volume = k8s.V1Volume(
    name="data-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(claim_name="airflow-ml-pvc")
)

volume_mount = k8s.V1VolumeMount(
    name="data-volume",
    mount_path="/data"
)

pod_override = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="base",
                image="dharineesh22/ml-image:1.1",
                volume_mounts=[volume_mount]
            )
        ],
        volumes=[volume]
    )
)


def extract_image(**kwargs):
    """Extract image and save as numpy file to shared storage"""
    from PIL import Image
    import numpy as np
    import os

    os.makedirs(TEMP_DIR, exist_ok=True)
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # Load image
    img = Image.open(IMAGE_PATH)
    arr = np.array(img)
    
    # Save to shared storage instead of XCom
    raw_image_path = f"{TEMP_DIR}/raw_image.npy"
    np.save(raw_image_path, arr)
    
    # Only push the file path (tiny) through XCom
    kwargs['ti'].xcom_push(key='raw_image_path', value=raw_image_path)
    
    print(f"Image shape: {arr.shape}")
    print(f"Saved raw image to: {raw_image_path}")


def convert_to_grayscale(**kwargs):
    """Load image from shared storage, convert to grayscale, save back"""
    import numpy as np
    import os

    os.makedirs(TEMP_DIR, exist_ok=True)
    
    # Get file path from XCom
    raw_image_path = kwargs['ti'].xcom_pull(key='raw_image_path')
    
    # Load from shared storage
    arr = np.load(raw_image_path)
    
    # Convert to grayscale
    gray = np.mean(arr, axis=2)
    
    # Save to shared storage
    gray_image_path = f"{TEMP_DIR}/gray_image.npy"
    np.save(gray_image_path, gray)
    
    # Push file path through XCom
    kwargs['ti'].xcom_push(key='gray_image_path', value=gray_image_path)
    
    print(f"Grayscale image shape: {gray.shape}")
    print(f"Saved grayscale image to: {gray_image_path}")


def save_output(**kwargs):
    """Load grayscale image from shared storage and save as PNG"""
    from PIL import Image
    import numpy as np
    import os

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # Get file path from XCom
    gray_image_path = kwargs['ti'].xcom_pull(key='gray_image_path')
    
    # Load from shared storage
    gray = np.load(gray_image_path)
    
    # Save as image
    img = Image.fromarray(gray.astype("uint8"))
    output_path = f"{OUTPUT_DIR}/output_grayscale.png"
    img.save(output_path)
    
    print(f"Saved final output to: {output_path}")
    
    # Optional: Clean up temporary files
    try:
        raw_path = kwargs['ti'].xcom_pull(key='raw_image_path')
        if raw_path and os.path.exists(raw_path):
            os.remove(raw_path)
        if os.path.exists(gray_image_path):
            os.remove(gray_image_path)
        print("Cleaned up temporary files")
    except Exception as e:
        print(f"Warning: Could not clean up temp files: {e}")


with DAG(
    dag_id="ml_image_grayscale_etl",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=extract_image,
        executor_config={"pod_override": pod_override},
    )

    grayscale = PythonOperator(
        task_id="grayscale",
        python_callable=convert_to_grayscale,
        executor_config={"pod_override": pod_override},
    )

    save = PythonOperator(
        task_id="save",
        python_callable=save_output,
        executor_config={"pod_override": pod_override},
    )

    extract >> grayscale >> save