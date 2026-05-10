from __future__ import annotations

import csv
import os
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from collection_utils import OUT, USER_AGENT, ensure_out, upload_to_s3


def collect_google_books(**context: Any) -> str:
    import requests

    ensure_out()
    params = {"q": "data engineering", "maxResults": "10"}
    api_key = os.environ.get("GOOGLE_BOOKS_API_KEY", "").strip()
    if api_key:
        params["key"] = api_key

    response = requests.get(
        "https://www.googleapis.com/books/v1/volumes",
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    response.raise_for_status()
    path = os.path.join(OUT, f"google_books_{context['ds_nodash']}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["id", "title", "publisher", "publishedDate", "previewLink"])
        writer.writeheader()
        for item in response.json().get("items", []):
            volume = item.get("volumeInfo") or {}
            writer.writerow(
                {
                    "id": item.get("id", ""),
                    "title": volume.get("title", ""),
                    "publisher": volume.get("publisher", ""),
                    "publishedDate": volume.get("publishedDate", ""),
                    "previewLink": volume.get("previewLink", ""),
                }
            )
    return path


def upload_google_books(**context: Any) -> None:
    upload_to_s3(context["ti"].xcom_pull(task_ids="collect_google_books"), "google-books")


with DAG(
    dag_id="google_public_api_collection_to_s3",
    default_args={"owner": "airflow", "retries": 1},
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["google", "public-api", "s3", "mlops"],
) as dag:
    collect = PythonOperator(task_id="collect_google_books", python_callable=collect_google_books)
    upload = PythonOperator(task_id="upload_google_books_to_s3", python_callable=upload_google_books)
    collect >> upload
