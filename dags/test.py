from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os


def collect_users_from_api():
    """
    Simple API data collection function.
    This pulls users from a public API and stores the response as JSON.
    """

    api_url = "https://jsonplaceholder.typicode.com/users"

    response = requests.get(api_url, timeout=30)
    response.raise_for_status()

    users = response.json()

    output_dir = "/opt/airflow/data"
    os.makedirs(output_dir, exist_ok=True)

    output_file = f"{output_dir}/users.json"

    with open(output_file, "w") as file:
        json.dump(users, file, indent=4)

    print(f"Successfully collected {len(users)} users")
    print(f"Data saved to {output_file}")


default_args = {
    "owner": "rajini",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


with DAG(
    dag_id="api_user_collection_dag",
    default_args=default_args,
    description="Simple DAG to collect user data from API",
    start_date=datetime(2026, 5, 11),
    schedule="@daily",
    catchup=False,
    tags=["api", "data-collection", "users"],
) as dag:

    collect_users_task = PythonOperator(
        task_id="collect_users_from_api",
        python_callable=collect_users_from_api,
    )

    collect_users_task