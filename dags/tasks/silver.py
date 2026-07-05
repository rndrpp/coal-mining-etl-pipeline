import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

# MinIO client setup
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "coal-lake")

def transform_silver(**context):
    log.info("Starting Silver transformation...")

    execution_date = context['ds']  # format: YYYY-MM-DD
    bronze_path = f"bronze/ritase/dt={execution_date}/data.parquet"
    silver_path = f"silver/ritase/dt={execution_date}/data.parquet"

    # 1. Initialize Spark session
    spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Silver Transformation") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
    .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
    .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED") \
    .config("spark.sql.parquet.int96RebaseModeInRead", "CORRECTED") \
    .config("spark.sql.legacy.parquet.nanosAsLong", "true") \
    .getOrCreate()

    # 2. Read Bronze data from MinIO
    df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/{bronze_path}")
    log.info(f"Read {df.count()} rows from Bronze data at '{bronze_path}'.")

    # 3 Clean and transform data
    # 3a. Drop duplicates
    df =  df.dropDuplicates()
    log.info(f"After dropping duplicates, {df.count()} rows remain.")

    # 3b. Fill missing values with defaults
    df = df.fillna({'driver_name': 'UNKNOWN'})

    # 3c. Flag and separate rejected rows (e.g., rows with missing critical fields)
    df = df.withColumn('is_valid', F.when(F.col('production_volume_bcm') < 0, False).otherwise(True))

    invalid_count = df.filter(F.col('is_valid') == False).count()
    log.info(f"Found {invalid_count} invalid rows based on production_volume_bcm.")

    df = df.filter(F.col('is_valid') == True).drop('is_valid')

    # 3d. Standarize string fields to uppercase
    string_cols = ['work_shift', 'material_code', 'material_group', 
                   'loading_area', 'dumping_area', 'production_activity',
                   'sampling_status', 'truck_category', 'supervisor_name']
    
    for col in string_cols:
        df = df.withColumn(col, F.upper(F.trim(F.col(col))))

    # 3e. Convert operation_date from excel format to ISO format (YYYY-MM-DD)
    df = df.withColumn('operation_date', F.col('operation_date').cast('date'))
    df = df.withColumn('sample_updated_at', F.col('sample_updated_at').cast('timestamp'))

    # 4. Write Silver data back to MinIO
    df.write.mode('overwrite').parquet(f"s3a://{MINIO_BUCKET}/{silver_path}")
    log.info(f"Silver data saved to MinIO at '{silver_path}'.")

    spark.stop()
    log.info("Silver transformation complete.")