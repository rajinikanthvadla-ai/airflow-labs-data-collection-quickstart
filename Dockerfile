FROM apache/airflow:3.2.0-python3.12

RUN pip install --no-cache-dir \
    requests \
    beautifulsoup4 \
    pandas \
    boto3 \
    lxml \
    apache-airflow-providers-amazon

USER airflow
