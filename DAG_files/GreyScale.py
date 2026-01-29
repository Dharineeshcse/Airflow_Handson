from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from kubernetes.client import models as k8s

IMAGE_PATH = "/data/images/multicoreware_logo.jpg"
OUTPUT_DIR = "/data/output"
TEMP_DIR = "/data/temp"

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

    # Create directories with proper error handling
    try:
        os.makedirs(TEMP_DIR, exist_ok=True, mode=0o777)
        os.makedirs(OUTPUT_DIR, exist_ok=True, mode=0o777)
        print(f"Created directories: {TEMP_DIR}, {OUTPUT_DIR}")
    except Exception as e:
        print(f"Error creating directories: {e}")
        raise

    # Check if input image exists
    if not os.path.exists(IMAGE_PATH):
        raise FileNotFoundError(f"Input image not found at: {IMAGE_PATH}")
    
    print(f"Loading image from: {IMAGE_PATH}")
    
    # Load image
    img = Image.open(IMAGE_PATH)
    arr = np.array(img)
    print(f"Image loaded successfully. Shape: {arr.shape}, dtype: {arr.dtype}")
    
    # Save to shared storage instead of XCom
    raw_image_path = f"{TEMP_DIR}/raw_image.npy"
    np.save(raw_image_path, arr)
    
    # Verify file was written
    if not os.path.exists(raw_image_path):
        raise RuntimeError(f"Failed to write file: {raw_image_path}")
    
    file_size = os.path.getsize(raw_image_path)
    print(f"Saved raw image to: {raw_image_path} (size: {file_size} bytes)")
    
    # Only push the file path (tiny) through XCom
    kwargs['ti'].xcom_push(key='raw_image_path', value=raw_image_path)
    print(f"XCom pushed with key 'raw_image_path': {raw_image_path}")
    
    return raw_image_path  # Also return for logging


def convert_to_grayscale(**kwargs):
    """Load image from shared storage, convert to grayscale, save back"""
    import numpy as np
    import os

    os.makedirs(TEMP_DIR, exist_ok=True, mode=0o777)
    
    # Get file path from XCom with validation
    raw_image_path = kwargs['ti'].xcom_pull(key='raw_image_path', task_ids='extract')
    
    if raw_image_path is None:
        raise ValueError("No raw_image_path found in XCom. Extract task may have failed.")
    
    print(f"Retrieved XCom path: {raw_image_path}")
    
    # Verify file exists
    if not os.path.exists(raw_image_path):
        raise FileNotFoundError(f"Raw image file not found at: {raw_image_path}")
    
    print(f"Loading image from: {raw_image_path}")
    
    # Load from shared storage
    arr = np.load(raw_image_path)
    print(f"Loaded array shape: {arr.shape}, dtype: {arr.dtype}")
    
    # Convert to grayscale
    if len(arr.shape) == 3:  # RGB image
        gray = np.mean(arr, axis=2)
    elif len(arr.shape) == 2:  # Already grayscale
        gray = arr
    else:
        raise ValueError(f"Unexpected image shape: {arr.shape}")
    
    print(f"Converted to grayscale. Shape: {gray.shape}")
    
    # Save to shared storage
    gray_image_path = f"{TEMP_DIR}/gray_image.npy"
    np.save(gray_image_path, gray)
    
    # Verify file was written
    if not os.path.exists(gray_image_path):
        raise RuntimeError(f"Failed to write file: {gray_image_path}")
    
    file_size = os.path.getsize(gray_image_path)
    print(f"Saved grayscale image to: {gray_image_path} (size: {file_size} bytes)")
    
    # Push file path through XCom
    kwargs['ti'].xcom_push(key='gray_image_path', value=gray_image_path)
    print(f"XCom pushed with key 'gray_image_path': {gray_image_path}")
    
    return gray_image_path


def save_output(**kwargs):
    """Load grayscale image from shared storage and save as PNG"""
    from PIL import Image
    import numpy as np
    import os

    os.makedirs(OUTPUT_DIR, exist_ok=True, mode=0o777)
    
    # Get file path from XCom with validation
    gray_image_path = kwargs['ti'].xcom_pull(key='gray_image_path', task_ids='grayscale')
    
    if gray_image_path is None:
        raise ValueError("No gray_image_path found in XCom. Grayscale task may have failed.")
    
    print(f"Retrieved XCom path: {gray_image_path}")
    
    # Verify file exists
    if not os.path.exists(gray_image_path):
        raise FileNotFoundError(f"Grayscale image file not found at: {gray_image_path}")
    
    print(f"Loading grayscale image from: {gray_image_path}")
    
    # Load from shared storage
    gray = np.load(gray_image_path)
    print(f"Loaded grayscale array shape: {gray.shape}, dtype: {gray.dtype}")
    
    # Save as image
    img = Image.fromarray(gray.astype("uint8"))
    output_path = f"{OUTPUT_DIR}/output_grayscale.png"
    img.save(output_path)
    
    # Verify output file
    if not os.path.exists(output_path):
        raise RuntimeError(f"Failed to save output image: {output_path}")
    
    file_size = os.path.getsize(output_path)
    print(f"Saved final output to: {output_path} (size: {file_size} bytes)")
    
    # Optional: Clean up temporary files
    try:
        raw_path = kwargs['ti'].xcom_pull(key='raw_image_path', task_ids='extract')
        if raw_path and os.path.exists(raw_path):
            os.remove(raw_path)
            print(f"Removed temporary file: {raw_path}")
        if os.path.exists(gray_image_path):
            os.remove(gray_image_path)
            print(f"Removed temporary file: {gray_image_path}")
        print("Cleaned up temporary files")
    except Exception as e:
        print(f"Warning: Could not clean up temp files: {e}")
    
    return output_path


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
