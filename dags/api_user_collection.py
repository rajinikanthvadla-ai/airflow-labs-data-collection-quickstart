from __future__ import annotations

import csv
import os
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from collection_utils import OUT, USER_AGENT, ensure_out, upload_to_s3


def collect_public_user_api(**context: Any) -> str:
    import requests

    ensure_out()
    response = requests.get(
        "https://jsonplaceholder.typicode.com/users",
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    path = os.path.join(OUT, f"public_users_api_{context['ds_nodash']}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "name", "username", "email", "city", "company"])
        writer.writeheader()
        for row in response.json():
            writer.writerow(
                {
                    "id": row.get("id", ""),
                    "name": row.get("name", ""),
                    "username": row.get("username", ""),
                    "email": row.get("email", ""),
                    "city": (row.get("address") or {}).get("city", ""),
                    "company": (row.get("company") or {}).get("name", ""),
                }
            )
    return path


def upload_public_user_api(**context: Any) -> None:
    upload_to_s3(context["ti"].xcom_pull(task_ids="collect_public_user_api"), "api-users")


with DAG(
    dag_id="api_user_collection_to_s3",
    default_args={"owner": "airflow", "retries": 1},
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["api", "users", "s3", "mlops"],
) as dag:
    collect = PythonOperator(task_id="collect_public_user_api", python_callable=collect_public_user_api)
    upload = PythonOperator(task_id="upload_public_user_api_to_s3", python_callable=upload_public_user_api)
    collect >> upload
