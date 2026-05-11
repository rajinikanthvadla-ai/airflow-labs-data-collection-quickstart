from __future__ import annotations

import csv
import os
from typing import Any

import boto3
import pendulum
import requests

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator


OUT = "/tmp/airflow-data"
USER_AGENT = "airflow-api-user-collection/1.0"


def ensure_out() -> None:
    os.makedirs(OUT, exist_ok=True)


def collect_public_user_api(**context: Any) -> str:
    """
    Collect public user data from API and save as CSV.
    """

    ensure_out()

    response = requests.get(
        "https://jsonplaceholder.typicode.com/users",
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()

    file_name = f"public_users_api_{context['ds_nodash']}.csv"
    file_path = os.path.join(OUT, file_name)

    with open(file_path, "w", newline="", encoding="utf-8") as file:
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

    print(f"CSV created successfully: {file_path}")
    return file_path


def upload_public_user_api_to_s3(**context: Any) -> None:
    """
    Upload collected CSV file to S3 using Airflow Variables.
    """

    ti = context["ti"]
    file_path = ti.xcom_pull(task_ids="collect_public_user_api")

    if not file_path:
        raise ValueError("No file path received from collect task")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"CSV file not found: {file_path}")

    aws_access_key_id = Variable.get("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = Variable.get("AWS_SECRET_ACCESS_KEY")
    aws_region = Variable.get("AWS_DEFAULT_REGION", default_var="ap-south-1")
    s3_bucket = Variable.get("S3_BUCKET")
    s3_prefix = Variable.get("S3_PREFIX", default_var="airflow-collected")

    file_name = os.path.basename(file_path)
    s3_key = f"{s3_prefix}/api-users/{file_name}"

    s3_client = boto3.client(
        "s3",
        region_name=aws_region,
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
    )

    s3_client.upload_file(file_path, s3_bucket, s3_key)

    print(f"Uploaded successfully to s3://{s3_bucket}/{s3_key}")


with DAG(
    dag_id="api_user_collection_to_s3",
    description="Collect users from public API, store as CSV, and upload to S3",
    default_args={
        "owner": "airflow",
        "retries": 1,
    },
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["api", "users", "csv", "s3", "mlops"],
) as dag:

    collect = PythonOperator(
        task_id="collect_public_user_api",
        python_callable=collect_public_user_api,
    )

    upload = PythonOperator(
        task_id="upload_public_user_api_to_s3",
        python_callable=upload_public_user_api_to_s3,
    )

    collect >> upload
