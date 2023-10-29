import time

from utils.request_utils import get_task_status


def check_task_status(task_id: str, task_status: str = "created") -> dict:
    if task_status in ["failed", "unknown"]:
        raise Exception("Task failed")

    res = get_task_status(task_id)
    task_status = res["task_status"]
    while task_status not in ["completed"]:
        res = get_task_status(task_id)
        task_status = res["task_status"]

        time.sleep(10)

    return res
