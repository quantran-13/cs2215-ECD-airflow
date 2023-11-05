from utils.request_utils import send_request
from utils.task_utils import check_task_status


def batch_predict_batch_pipeline(
    ti,
    forecasting_horizon: int = 24,
    url: str = "http://100.113.148.58:8001/clearml/v1/batch_prediction_pipeline/batch_prediction",
) -> dict:
    data_task_id = ti.xcom_pull(key="task_id", task_ids=["load"])[0]
    training_task_id = ti.xcom_pull(key="task_id", task_ids=["train"])[0]
    payload = {
        "data_task_id": data_task_id,
        "training_task_id": training_task_id,
        "forecasting_horizon": forecasting_horizon,
    }

    res = send_request(url, payload)

    task_id = res["task_id"]
    task_status = res["task_status"]
    res = check_task_status(task_id, task_status)

    task_id = res["task_id"]
    ti.xcom_push(key="task_id", value=task_id)

    return res
