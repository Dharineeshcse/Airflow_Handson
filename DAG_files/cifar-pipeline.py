from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes.client import V1Volume, V1VolumeMount, V1PersistentVolumeClaimVolumeSource
from datetime import datetime

ML_IMAGE = "dharineesh22/cifar-model-image:1.4"
PVC_NAME = "datapipeline-ml-pvc"
PVC_MOUNT_PATH = "/mnt/airflow-ml"

volume = V1Volume(
    name="airflow-ml-volume",
    persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=PVC_NAME),
)
volume_mount = V1VolumeMount(name="airflow-ml-volume", mount_path=PVC_MOUNT_PATH)

with DAG(
    dag_id="ml_training_pipeline",
    start_date=datetime(2024,1,1),
    schedule=None,
    default_args={"executor":"KubernetesExecutor"},
    catchup=False
) as dag:

    prepare = KubernetesPodOperator(
        task_id="prepare_dataset",
        name="prepare-dataset",
        namespace="airflow",
        image=ML_IMAGE,
        cmds=["python3", "/app/train.py", "prepare_dataset"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    train = KubernetesPodOperator(
        task_id="train_model",
        name="train-model",
        namespace="airflow",
        image=ML_IMAGE,
        cmds=["python3", "/app/train.py", "train"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    infer = KubernetesPodOperator(
        task_id="run_inference",
        name="run-inference",
        namespace="airflow",
        image=ML_IMAGE,
        cmds=["python3", "/app/train.py", "inference"],
        volumes=[volume],
        volume_mounts=[volume_mount],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    prepare >> train >> infer
