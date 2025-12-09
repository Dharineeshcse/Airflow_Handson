from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator
from datetime import datetime

ML_IMAGE = "dharineesh22/cifar-model-image:1.1"
PVC_NAME = "datapipeline-ml-pvc"
ML_VOLUME_MOUNT_PATH = "/mnt/airflow-ml"

with DAG(
    dag_id="ml_training_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    default_args={"executor": "KubernetesExecutor"},
    catchup=False
) as dag:

    prepare = KubernetesPodOperator(
        task_id="prepare_dataset",
        name="prepare-dataset",
        namespace="airflow",
        image=ML_IMAGE,
        cmds=["python3", "/app/train.py"],
        arguments=["prepare_dataset"],
        volume_mounts=[{
            "name": "airflow-ml-volume",
            "mountPath": ML_VOLUME_MOUNT_PATH
        }],
        volumes=[{
            "name": "airflow-ml-volume",
            "persistentVolumeClaim": {"claimName": PVC_NAME}
        }],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    train = KubernetesPodOperator(
        task_id="train_model",
        name="train-model",
        namespace="airflow",
        image=ML_IMAGE,
        cmds=["python3", "/app/train.py"],
        arguments=["train"],
        volume_mounts=[{
            "name": "airflow-ml-volume",
            "mountPath": ML_VOLUME_MOUNT_PATH
        }],
        volumes=[{
            "name": "airflow-ml-volume",
            "persistentVolumeClaim": {"claimName": PVC_NAME}
        }],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    infer = KubernetesPodOperator(
        task_id="run_inference",
        name="run-inference",
        namespace="airflow",
        image=ML_IMAGE,
        cmds=["python3", "/app/train.py"],
        arguments=["inference"],
        volume_mounts=[{
            "name": "airflow-ml-volume",
            "mountPath": ML_VOLUME_MOUNT_PATH
        }],
        volumes=[{
            "name": "airflow-ml-volume",
            "persistentVolumeClaim": {"claimName": PVC_NAME}
        }],
        get_logs=True,
        is_delete_operator_pod=True,
    )

    prepare >> train >> infer
