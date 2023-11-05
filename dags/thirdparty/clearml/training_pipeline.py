from utils.request_utils import send_request
from utils.task_utils import check_task_status


def hpo_training_pipeline(
    ti,
    forecasting_horizon: int = 24,
    k: int = 3,
    lag_feature_lag_min: int = 1,
    lag_feature_lag_max: int = 72,
    url: str = "http://100.113.148.58:8001/clearml/v1/training_pipeline/hpo",
) -> dict:
    artifacts_task_id = ti.xcom_pull(key="task_id", task_ids=["load"])[0]
    payload = {
        "artifacts_task_id": artifacts_task_id,
        "forecasting_horizon": forecasting_horizon,
        "k": k,
        "lag_feature_lag_min": lag_feature_lag_min,
        "lag_feature_lag_max": lag_feature_lag_max,
    }

    res = send_request(url, payload)

    task_id = res["task_id"]
    task_status = res["task_status"]
    res = check_task_status(task_id, task_status)

    task_id = res["task_id"]
    ti.xcom_push(key="task_id", value=task_id)

    return res


def train_training_pipeline(
    ti,
    forecasting_horizon: int = 24,
    url: str = "http://100.113.148.58:8001/clearml/v1/training_pipeline/train",
) -> dict:
    artifacts_task_id = ti.xcom_pull(key="task_id", task_ids=["load"])[0]
    hpo_task_id = ti.xcom_pull(key="task_id", task_ids=["hpo"])[0]
    payload = {
        "artifacts_task_id": artifacts_task_id,
        "hpo_task_id": hpo_task_id,
        "forecasting_horizon": forecasting_horizon,
    }

    res = send_request(url, payload)

    task_id = res["task_id"]
    task_status = res["task_status"]
    res = check_task_status(task_id, task_status)

    task_id = res["task_id"]
    ti.xcom_push(key="task_id", value=task_id)

    return res
