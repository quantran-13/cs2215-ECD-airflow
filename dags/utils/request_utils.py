import json

import requests
from utils.logger import get_logger

app_logger = get_logger("logs")


def send_request(url: str, payload: dict) -> dict:
    app_logger.info(f"Sending request to {url} with payload: {payload}")

    headers = {"Content-Type": "application/json"}
    response = requests.post(url, data=json.dumps(payload), headers=headers)
    if response.status_code != 200:
        app_logger.error(f"Request failed with status code: {response.status_code}")
        raise Exception(f"Request failed with status code: {response.status_code}")

    app_logger.info(f"Request successful with status code: {response.status_code}")
    app_logger.info(f"Response: {response.json()}")

    return response.json()


def get_task_status(task_id: str, url: str = "http://100.113.148.58:8001/clearml/v1/status") -> dict:
    params = {"task_id": task_id}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        app_logger.error(f"Request failed with status code: {response.status_code}")
        raise Exception(f"Request failed with status code: {response.status_code}")

    app_logger.info(f"Response: {response.json()}")

    return response.json()


def get_task_metadata(
    task_id: str,
    url: str = "http://100.113.148.58:8001/clearml/v1/get_metadata",
) -> dict:
    params = {"task_id": task_id}

    response = requests.get(url, params=params)

    if response.status_code != 200:
        app_logger.error(f"Request failed with status code: {response.status_code}")
        raise Exception(f"Request failed with status code: {response.status_code}")

    app_logger.info(f"Response: {response.json()}")

    return response.json()
