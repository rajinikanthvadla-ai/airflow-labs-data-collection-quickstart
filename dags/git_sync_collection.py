from __future__ import annotations

import csv
import os
import urllib.parse
from datetime import datetime
from typing import Any
from urllib.robotparser import RobotFileParser

import pendulum
from airflow import DAG
from airflow.operators.python import PythonOperator

OUT = "/opt/airflow/data"
USER_AGENT = "AirflowCollectionDAG/1.0 (+https://airflow.apache.org)"


def _ensure_out() -> None:
    os.makedirs(OUT, exist_ok=True)


def collect_rest_api(**context: Any) -> None:
    import requests

    _ensure_out()
    r = requests.get(
        "https://jsonplaceholder.typicode.com/posts",
        params={"_limit": "8"},
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    r.raise_for_status()
    rows = r.json()
    path = os.path.join(OUT, f"rest_api_{context['ds_nodash']}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["userId", "id", "title", "body"])
        w.writeheader()
        for row in rows:
            w.writerow({k: row.get(k, "") for k in ["userId", "id", "title", "body"]})


def scrape_static_page(**context: Any) -> None:
    import requests
    from bs4 import BeautifulSoup

    _ensure_out()
    url = "https://quotes.toscrape.com/"
    r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
    r.raise_for_status()
    soup = BeautifulSoup(r.text, "lxml")
    quotes = []
    for q in soup.select("div.quote"):
        text = (q.select_one("span.text").get_text(strip=True) if q.select_one("span.text") else "")
        author = (q.select_one("small.author").get_text(strip=True) if q.select_one("small.author") else "")
        tags = ",".join(t.get_text(strip=True) for t in q.select("a.tag"))
        quotes.append({"page_url": url, "quote": text, "author": author, "tags": tags})
    path = os.path.join(OUT, f"scrape_{context['ds_nodash']}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["page_url", "quote", "author", "tags"])
        w.writeheader()
        w.writerows(quotes)


def crawl_same_host_light(**context: Any) -> None:
    import requests
    from bs4 import BeautifulSoup

    _ensure_out()
    start = "https://quotes.toscrape.com/"
    parsed = urllib.parse.urlparse(start)
    base = f"{parsed.scheme}://{parsed.netloc}"
    rp = RobotFileParser()
    rp.set_url(urllib.parse.urljoin(base, "/robots.txt"))
    rp.read()
    if not rp.can_fetch(USER_AGENT, start):
        raise RuntimeError("robots.txt disallows fetch for this user-agent")

    seen: set[str] = set()
    frontier = [start]
    rows: list[dict[str, str]] = []
    max_pages = 4
    while frontier and len(seen) < max_pages:
        url = frontier.pop(0)
        if url in seen:
            continue
        if not url.startswith(base):
            continue
        if not rp.can_fetch(USER_AGENT, url):
            continue
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        seen.add(url)
        soup = BeautifulSoup(r.text, "lxml")
        for a in soup.select("a[href]"):
            href = a.get("href", "").strip()
            nxt = urllib.parse.urljoin(url, href)
            if nxt.startswith(base) and nxt not in seen and len(seen) + len(frontier) < max_pages:
                if rp.can_fetch(USER_AGENT, nxt):
                    frontier.append(nxt)
        title = soup.title.get_text(strip=True) if soup.title else ""
        rows.append({"url": url, "title": title, "html_len": str(len(r.text))})
    path = os.path.join(OUT, f"crawl_{context['ds_nodash']}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["url", "title", "html_len"])
        w.writeheader()
        w.writerows(rows)


def collect_google_books_public(**context: Any) -> None:
    import requests

    _ensure_out()
    # Public Books API JSON; optional GOOGLE_BOOKS_API_KEY raises quota if set in Airflow env/Variable
    key = os.environ.get("GOOGLE_BOOKS_API_KEY", "").strip()
    params = {"q": "data engineering", "maxResults": "5"}
    if key:
        params["key"] = key
    r = requests.get(
        "https://www.googleapis.com/books/v1/volumes",
        params=params,
        headers={"User-Agent": USER_AGENT},
        timeout=30,
    )
    r.raise_for_status()
    payload = r.json()
    items = payload.get("items") or []
    path = os.path.join(OUT, f"google_books_{context['ds_nodash']}.csv")
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["id", "title", "publisher", "publishedDate", "previewLink"])
        w.writeheader()
        for it in items:
            vol = it.get("volumeInfo") or {}
            w.writerow(
                {
                    "id": it.get("id", ""),
                    "title": vol.get("title", ""),
                    "publisher": vol.get("publisher", ""),
                    "publishedDate": vol.get("publishedDate", ""),
                    "previewLink": vol.get("previewLink", ""),
                }
            )


def optional_upload_s3(**_: Any) -> None:
    key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not (key and secret and bucket):
        return
    import boto3

    _ensure_out()
    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip() or "us-east-1"
    prefix = os.environ.get("S3_PREFIX", "airflow-collected").strip().strip("/")
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "").strip() or None
    client_kw: dict[str, Any] = {"region_name": region}
    if endpoint:
        client_kw["endpoint_url"] = endpoint
    s3 = boto3.client("s3", **client_kw)
    for name in sorted(os.listdir(OUT)):
        if not name.endswith(".csv"):
            continue
        full = os.path.join(OUT, name)
        key_name = f"{prefix}/{name}"
        s3.upload_file(full, bucket, key_name)


default_args = {"owner": "airflow", "retries": 1}

with DAG(
    dag_id="git_sync_data_collection",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    tags=["git-sync", "collection", "kubernetes"],
) as dag:
    t_rest = PythonOperator(task_id="rest_public_api", python_callable=collect_rest_api)
    t_scrape = PythonOperator(task_id="scrape_static_html", python_callable=scrape_static_page)
    t_crawl = PythonOperator(task_id="crawl_same_site", python_callable=crawl_same_host_light)
    t_google = PythonOperator(task_id="google_books_api", python_callable=collect_google_books_public)
    t_s3 = PythonOperator(task_id="optional_s3_upload", python_callable=optional_upload_s3)
    [t_rest, t_scrape, t_crawl, t_google] >> t_s3
