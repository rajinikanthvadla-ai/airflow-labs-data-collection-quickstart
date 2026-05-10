from __future__ import annotations

import csv
import os
from typing import Any

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from collection_utils import OUT, USER_AGENT, ensure_out, upload_to_s3


def scrape_quotes_page(**context: Any) -> str:
    import requests
    from bs4 import BeautifulSoup

    ensure_out()
    url = "https://quotes.toscrape.com/"
    response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "lxml")
    path = os.path.join(OUT, f"scraped_quotes_{context['ds_nodash']}.csv")

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["page_url", "quote", "author", "tags"])
        writer.writeheader()
        for quote in soup.select("div.quote"):
            text = quote.select_one("span.text")
            author = quote.select_one("small.author")
            writer.writerow(
                {
                    "page_url": url,
                    "quote": text.get_text(strip=True) if text else "",
                    "author": author.get_text(strip=True) if author else "",
                    "tags": ",".join(tag.get_text(strip=True) for tag in quote.select("a.tag")),
                }
            )
    return path


def upload_scraped_quotes(**context: Any) -> None:
    upload_to_s3(context["ti"].xcom_pull(task_ids="scrape_quotes_page"), "scraping")


with DAG(
    dag_id="scraping_collection_to_s3",
    default_args={"owner": "airflow", "retries": 1},
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["scraping", "s3", "mlops"],
) as dag:
    scrape = PythonOperator(task_id="scrape_quotes_page", python_callable=scrape_quotes_page)
    upload = PythonOperator(task_id="upload_scraped_quotes_to_s3", python_callable=upload_scraped_quotes)
    scrape >> upload
