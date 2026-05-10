from __future__ import annotations

import os
from typing import Any

OUT = "/opt/airflow/data"
USER_AGENT = "AirflowCollectionDAG/1.0 (+https://airflow.apache.org)"


def ensure_out() -> None:
    os.makedirs(OUT, exist_ok=True)


def upload_to_s3(local_path: str, s3_folder: str) -> None:
    access_key = os.environ.get("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.environ.get("AWS_SECRET_ACCESS_KEY", "").strip()
    bucket = os.environ.get("S3_BUCKET", "").strip()
    if not (access_key and secret_key and bucket):
        raise RuntimeError("Set AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and S3_BUCKET on Airflow workers")

    import boto3

    region = os.environ.get("AWS_DEFAULT_REGION", "us-east-1").strip() or "us-east-1"
    prefix = os.environ.get("S3_PREFIX", "airflow-collected").strip().strip("/")
    endpoint = os.environ.get("AWS_ENDPOINT_URL", "").strip() or None
    client_args: dict[str, Any] = {"region_name": region}
    if endpoint:
        client_args["endpoint_url"] = endpoint

    s3_key = f"{prefix}/{s3_folder.strip('/')}/{os.path.basename(local_path)}"
    boto3.client("s3", **client_args).upload_file(local_path, bucket, s3_key)
