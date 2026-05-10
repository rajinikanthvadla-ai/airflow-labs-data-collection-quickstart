from __future__ import annotations

import csv
import os
import urllib.parse
from typing import Any
from urllib.robotparser import RobotFileParser

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator
from collection_utils import OUT, USER_AGENT, ensure_out, upload_to_s3


def crawl_quotes_site(**context: Any) -> str:
    import requests
    from bs4 import BeautifulSoup

    ensure_out()
    start = "https://quotes.toscrape.com/"
    base = f"{urllib.parse.urlparse(start).scheme}://{urllib.parse.urlparse(start).netloc}"
    robots = RobotFileParser()
    robots.set_url(urllib.parse.urljoin(base, "/robots.txt"))
    robots.read()
    if not robots.can_fetch(USER_AGENT, start):
        raise RuntimeError("robots.txt disallows fetch for this user-agent")

    seen: set[str] = set()
    frontier = [start]
    path = os.path.join(OUT, f"crawled_pages_{context['ds_nodash']}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["url", "title", "html_len"])
        writer.writeheader()
        while frontier and len(seen) < 4:
            url = frontier.pop(0)
            if url in seen or not url.startswith(base) or not robots.can_fetch(USER_AGENT, url):
                continue
            response = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
            response.raise_for_status()
            seen.add(url)
            soup = BeautifulSoup(response.text, "lxml")
            writer.writerow(
                {
                    "url": url,
                    "title": soup.title.get_text(strip=True) if soup.title else "",
                    "html_len": len(response.text),
                }
            )
            for link in soup.select("a[href]"):
                next_url = urllib.parse.urljoin(url, link.get("href", "").strip())
                if next_url.startswith(base) and next_url not in seen and len(seen) + len(frontier) < 4:
                    frontier.append(next_url)
    return path


def upload_crawled_pages(**context: Any) -> None:
    upload_to_s3(context["ti"].xcom_pull(task_ids="crawl_quotes_site"), "crawling")


with DAG(
    dag_id="crawling_collection_to_s3",
    default_args={"owner": "airflow", "retries": 1},
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["crawling", "s3", "mlops"],
) as dag:
    crawl = PythonOperator(task_id="crawl_quotes_site", python_callable=crawl_quotes_site)
    upload = PythonOperator(task_id="upload_crawled_pages_to_s3", python_callable=upload_crawled_pages)
    crawl >> upload
