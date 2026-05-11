from __future__ import annotations

import json
import os
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from collection_utils import OUT, ensure_out, upload_to_s3


def collect_users_from_api(**context: Any) -> str:
    import requests

    ensure_out()
    response = requests.get("https://jsonplaceholder.typicode.com/users", timeout=30)
    response.raise_for_status()

    path = os.path.join(OUT, f"test_users_{context['ds_nodash']}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(response.json(), f, indent=2)
    return path


def upload_test_users(**context: Any) -> None:
    upload_to_s3(context["ti"].xcom_pull(task_ids="collect_users"), "test-users")


with DAG(
    dag_id="test_api_user_collection_to_s3",
    default_args={"owner": "airflow", "retries": 1},
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["test", "api", "s3"],
) as dag:
    collect = PythonOperator(task_id="collect_users", python_callable=collect_users_from_api)
    upload = PythonOperator(task_id="upload_test_users_to_s3", python_callable=upload_test_users)
    collect >> upload
