from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os


DATA_PATH = "/opt/airflow/mlops-data"
OUTPUT_PATH = "/opt/airflow/output"


def collect_data():
    customers = pd.read_csv(f"{DATA_PATH}/customers.csv")
    orders = pd.read_csv(f"{DATA_PATH}/orders.csv")
    payments = pd.read_csv(f"{DATA_PATH}/payments.csv")
    tickets = pd.read_csv(f"{DATA_PATH}/support_tickets.csv")

    order_features = orders.groupby("customer_id").agg(
        total_orders=("order_id", "count"),
        total_amount=("amount", "sum"),
        avg_order_value=("amount", "mean"),
        cancelled_orders=("order_status", lambda x: (x == "cancelled").sum())
    ).reset_index()

    payment_features = payments.groupby("customer_id").agg(
        failed_payments=("payment_status", lambda x: (x == "failed").sum())
    ).reset_index()

    ticket_features = tickets.groupby("customer_id").agg(
        support_ticket_count=("ticket_id", "count"),
        negative_tickets=("sentiment", lambda x: (x == "negative").sum())
    ).reset_index()

    final_df = customers.merge(order_features, on="customer_id", how="left")
    final_df = final_df.merge(payment_features, on="customer_id", how="left")
    final_df = final_df.merge(ticket_features, on="customer_id", how="left")

    final_df = final_df.fillna(0)

    final_df["churn_label"] = final_df.apply(
        lambda row: 1
        if row["cancelled_orders"] > 0 or row["failed_payments"] > 0 or row["negative_tickets"] > 0
        else 0,
        axis=1
    )

    os.makedirs(OUTPUT_PATH, exist_ok=True)
    final_df.to_csv(f"{OUTPUT_PATH}/ml_training_dataset.csv", index=False)

    print("Data collection completed")
    print(final_df)


def validate_data():
    dataset_path = f"{OUTPUT_PATH}/ml_training_dataset.csv"

    if not os.path.exists(dataset_path):
        raise FileNotFoundError("Training dataset not found")

    df = pd.read_csv(dataset_path)

    required_columns = [
        "customer_id",
        "name",
        "city",
        "signup_days",
        "total_orders",
        "total_amount",
        "avg_order_value",
        "cancelled_orders",
        "failed_payments",
        "support_ticket_count",
        "negative_tickets",
        "churn_label"
    ]

    missing_columns = [col for col in required_columns if col not in df.columns]

    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    if df["customer_id"].duplicated().any():
        raise ValueError("Duplicate customer_id found")

    if df["total_amount"].min() < 0:
        raise ValueError("Negative amount found")

    if df.isnull().sum().sum() > 0:
        raise ValueError("Null values found")

    print("Data validation passed")
    print("Rows:", len(df))
    print("Columns:", len(df.columns))


with DAG(
    dag_id="mlops_data_collection_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["mlops", "data-collection", "training-dataset"],
) as dag:

    collect_data_task = PythonOperator(
        task_id="collect_data_from_multiple_sources",
        python_callable=collect_data
    )

    validate_data_task = PythonOperator(
        task_id="validate_training_dataset",
        python_callable=validate_data
    )

    collect_data_task >> validate_data_task
