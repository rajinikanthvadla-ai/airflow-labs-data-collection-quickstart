from __future__ import annotations

import csv
import os
from typing import Any

import pendulum
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator


OUT = "/tmp/airflow-data"
USER_AGENT = "airflow-api-user-collection/1.0"


def ensure_out() -> None:
    os.makedirs(OUT, exist_ok=True)


def collect_public_user_api(**context: Any) -> str:
    """
    Collect public users from API and save as CSV.
    """

    ensure_out()

    response = requests.get(
        "https://jsonplaceholder.typicode.com/users",
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()

    execution_date = context["ds_nodash"]
    path = os.path.join(OUT, f"public_users_api_{execution_date}.csv")

    with open(path, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "id",
                "name",
                "username",
                "email",
                "city",
                "company",
            ],
        )

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

    print(f"CSV file created successfully: {path}")
    return path


def validate_file(**context: Any) -> None:
    """
    Validate CSV file exists before next step.
    """

    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="collect_public_user_api")

    if not file_path:
        raise ValueError("No file path received from collect task")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    print(f"Validated file: {file_path}")


with DAG(
    dag_id="api_user_collection_to_local_csv",
    description="Collect public user data from API and save as CSV file",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["api", "users", "csv", "mlops"],
) as dag:

    collect = PythonOperator(
        task_id="collect_public_user_api",
        python_callable=collect_public_user_api,
    )

    validate = PythonOperator(
        task_id="validate_collected_csv_file",
        python_callable=validate_file,
    )

    collect >> validate
