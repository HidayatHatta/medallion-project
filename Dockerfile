FROM apache/airflow:3.3.2
RUN pip install --no-cache-dir duckdb==1.5.6 boto3==1.43.112 requests==2.34.2