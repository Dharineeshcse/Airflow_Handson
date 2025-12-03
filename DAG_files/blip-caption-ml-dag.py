# ml_blip_captioning_dag.py
from datetime import datetime
from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from kubernetes import client as k8s

IMAGE_PATH = "/data/images/AWS_logo.png"
OUTPUT_DIR = "/data/output"
TEMP_DIR = "/data/temp"

# PVC volume (uses your provided claim name)
volume = k8s.V1Volume(
    name="data-volume",
    persistent_volume_claim=k8s.V1PersistentVolumeClaimVolumeSource(
        claim_name="airflow-ml-pvc"
    )
)

volume_mount = k8s.V1VolumeMount(
    name="data-volume",
    mount_path="/data"
)

# --------
# NOTE: This DAG uses the CPU image by default. If you build a GPU-enabled image (see notes),
# add resource limits: resources = k8s.V1ResourceRequirements(limits={"nvidia.com/gpu": "1"})
# and include `resources=resources` in V1Container below and in KubernetesPodOperator.
# --------

pod_override = k8s.V1Pod(
    spec=k8s.V1PodSpec(
        containers=[
            k8s.V1Container(
                name="blip-container",
                image="dharineesh22/blip-image:1.0",   # replace with your image
                volume_mounts=[volume_mount],
                # resources=resources,  # uncomment if using GPU-enabled image
            )
        ],
        volumes=[volume],
        restart_policy="Never"
    )
)

default_args = {"owner": "airflow"}

with DAG(
    dag_id="ml_blip_image_captioning",
    start_date=datetime(2024, 12, 1),
    schedule=None,
    catchup=False,
    default_args=default_args,
) as dag:

    run_caption = KubernetesPodOperator(
        task_id="run_blip_caption",
        name="blip-image-caption",
        namespace="default",
        image="dharineesh22/blip-image:1.0",   # your image on dockerhub
        cmds=["python3", "-u", "/app/infer_blip.py"],
        arguments=[IMAGE_PATH, OUTPUT_DIR],
        volumes=[volume],
        volume_mounts=[volume_mount],
        pod_override=pod_override,
        get_logs=True,
        is_delete_operator_pod=True,
    )

    run_caption
