import os
import logging
import pandas as pd
from minio import Minio
from io import BytesIO

# Logging
log = logging.getLogger(__name__)

# MinIO client setup
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "coal-lake")

# Source file
SOURCE_PATH = "/opt/airflow/data/coal_mining_data.xlsx"
HEADER_ROW = 3  # Adjust based on your Excel file structure

def ingest_bronze(**context):
    log.info("Starting Bronze ingestion...")
    
    # 1. Read Excel
    df = pd.read_excel(SOURCE_PATH, header=HEADER_ROW)
    df.dropna(how='all', inplace=True)  # Drop rows where all elements are NaN
    df.dropna(axis=1, how='all', inplace=True)  # Drop columns where all elements are NaN
    log.info(f"Read {len(df)} rows from Excel file.")
    
    # 2. Ensure bucket exists
    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False
    )
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        log.info(f"Bucket '{MINIO_BUCKET}' created.")
    else:
        log.info(f"Bucket '{MINIO_BUCKET}' already exists.")

    # 3. Save to MinIO as Parquet
    execution_date = context['ds'] # format: YYYY-MM-DD
    object_name = f"bronze/ritase/dt={execution_date}/data.parquet"

    buffer = BytesIO()
    df.to_parquet(buffer, engine='pyarrow', index=False)
    buffer.seek(0)

    minio_client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type='application/octet-stream'
    )

    log.info(f"Bronze data saved to MinIO at '{object_name}'.")