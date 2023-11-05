from airflow.models import Variable
import time
import os
from utils.request_utils import get_task_metadata, send_request
from utils.task_utils import check_task_status


LOCK_FILE = "/opt/airflow/dags/feature_pipeline.lock"


def extract_feature_pipeline(
    ti,
    artifacts_task_id: str,
    days_delay: int = 15,
    days_export: int = 30,
    url: str = "http://100.113.148.58:8001/clearml/v1/feature_pipeline/extract",
) -> dict:
    # create tmp lock file
    while os.path.exists(path=LOCK_FILE):
        print("lock file exists, wait 60s")
        time.sleep(60)
    with open(file=LOCK_FILE, mode="w") as f:
        f.write("lock")

    execution_date = ti.execution_date.strftime("%Y-%m-%d %H:%M")
    feature_store_id = str(object=Variable.get(key="ml_pipeline_feature_store_id"))
    payload = {
        "artifacts_task_id": artifacts_task_id,
        "feature_store_id": feature_store_id,
        "export_end_reference_datetime": execution_date,
        "days_delay": days_delay,
        "days_export": days_export,
    }

    res = send_request(url=url, payload=payload)

    task_id = res["task_id"]
    task_status = res["task_status"]
    res = check_task_status(task_id=task_id, task_status=task_status)

    task_id = res["task_id"]
    ti.xcom_push(key="task_id", value=task_id)

    return res


def transform_feature_pipeline(
    ti,
    url: str = "http://100.113.148.58:8001/clearml/v1/feature_pipeline/transform",
) -> dict:
    artifacts_task_id = ti.xcom_pull(key="task_id", task_ids=["extract"])[0]
    payload = {"artifacts_task_id": artifacts_task_id}

    res = send_request(url=url, payload=payload)

    task_id = res["task_id"]
    task_status = res["task_status"]
    res = check_task_status(task_id=task_id, task_status=task_status)

    task_id = res["task_id"]
    ti.xcom_push(key="task_id", value=task_id)

    return res


def validate_feature_pipeline(
    ti,
    url: str = "http://100.113.148.58:8001/clearml/v1/feature_pipeline/validate",
) -> dict:
    artifacts_task_id = ti.xcom_pull(key="task_id", task_ids=["transform"])[0]
    payload = {"artifacts_task_id": artifacts_task_id}

    res = send_request(url=url, payload=payload)

    task_id = res["task_id"]
    task_status = res["task_status"]
    res = check_task_status(task_id=task_id, task_status=task_status)

    task_id = res["task_id"]
    ti.xcom_push(key="task_id", value=task_id)

    return res


def load_feature_pipeline(
    ti,
    feature_group_version: str,
    url: str = "http://100.113.148.58:8001/clearml/v1/feature_pipeline/load",
) -> dict:
    artifacts_task_id = ti.xcom_pull(key="task_id", task_ids=["validate"])[0]
    payload = {
        "artifacts_task_id": artifacts_task_id,
        "feature_group_version": feature_group_version,
    }

    res = send_request(url=url, payload=payload)

    task_id = res["task_id"]
    task_status = res["task_status"]
    res = check_task_status(task_id=task_id, task_status=task_status)

    task_id = res["task_id"]
    res = get_task_metadata(task_id=task_id)

    task_id = res["task_id"]
    metadata = res["metadata"]["feature_store_id"]
    Variable.set(key="ml_pipeline_feature_store_id", value=metadata)

    ti.xcom_push(key="task_id", value=task_id)

    os.remove(path=LOCK_FILE)

    return res
