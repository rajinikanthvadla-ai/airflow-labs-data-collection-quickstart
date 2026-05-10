FROM apache/airflow:2.10.4-python3.12
USER root
RUN pip install --no-cache-dir requests beautifulsoup4 pandas boto3 lxml
USER airflow
