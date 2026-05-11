from __future__ import annotations

import logging
import os
from typing import Any

from airflow.models import Variable

OUT = "/opt/airflow/data"
USER_AGENT = "AirflowCollectionDAG/1.0 (+https://airflow.apache.org)"
log = logging.getLogger(__name__)


def ensure_out() -> None:
    os.makedirs(OUT, exist_ok=True)


def get_aws_config() -> dict[str, str]:
    """Read AWS settings from Airflow Variables (Admin -> Variables in the UI)."""
    access_key = Variable.get("aws_access_key_id")
    secret_key = Variable.get("aws_secret_access_key")
    region = Variable.get("aws_default_region", default_var="us-east-1")
    bucket = Variable.get("s3_bucket")
    prefix = Variable.get("s3_prefix", default_var="airflow-collected")

    if not access_key or not secret_key or not bucket:
        raise RuntimeError(
            "Missing required Airflow Variables. Go to Admin -> Variables in the "
            "Airflow UI and set: aws_access_key_id, aws_secret_access_key, s3_bucket"
        )
    return {
        "aws_access_key_id": access_key.strip(),
        "aws_secret_access_key": secret_key.strip(),
        "aws_default_region": region.strip() or "us-east-1",
        "s3_bucket": bucket.strip(),
        "s3_prefix": prefix.strip().strip("/"),
    }


def upload_to_s3(local_path: str, s3_folder: str) -> None:
    """Upload a local file to S3 using credentials from Airflow Variables."""
    config = get_aws_config()

    import boto3

    s3_key = f"{config['s3_prefix']}/{s3_folder.strip('/')}/{os.path.basename(local_path)}"
    client = boto3.client(
        "s3",
        aws_access_key_id=config["aws_access_key_id"],
        aws_secret_access_key=config["aws_secret_access_key"],
        region_name=config["aws_default_region"],
    )
    client.upload_file(local_path, config["s3_bucket"], s3_key)
    log.info("Uploaded %s -> s3://%s/%s", local_path, config["s3_bucket"], s3_key)
