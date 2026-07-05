import os
import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

log = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "coal-lake")

def transform_gold(**context):
    log.info("Starting Gold transformation...")

    execution_date = context['ds']  # format: YYYY-MM-DD
    silver_path = f"silver/ritase/dt={execution_date}/data.parquet"

     # 1. Initialize Spark session
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("Gold Transformation") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

    # 2. Read Silver data from MinIO
    df = spark.read.parquet(f"s3a://{MINIO_BUCKET}/silver/ritase/dt={execution_date}/data.parquet")
    log.info(f"Read {df.count()} rows from Silver data.")

    # 3. gold_shift_production_volume_bcm: Calculate total production volume per work shift
    shift_production = df.groupBy('operation_date', 'work_shift', 'material_group'
                                ).agg(
                                    F.sum('production_volume_bcm').alias('total_volume_bcm'),
                                    F.count('*').alias('total_trips'),
                                    F.avg('production_volume_bcm').alias('avg_volume_per_trip'),
                                    F.sum('hauling_distance_m').alias('total_distance_m')
                                    )
    shift_production.write \
    .mode('overwrite') \
    .parquet(f"s3a://{MINIO_BUCKET}/gold/shift_production/dt={execution_date}/data.parquet")

    log.info(f"Gold shift production data written to 'gold/shift_production/dt={execution_date}/data.parquet'.")

    # 4. gold_daily_production_summary: Calculate daily production summary
    daily_production = df.groupBy('operation_date', 'material_group'
                                ).agg(
                                    F.sum('production_volume_bcm').alias('total_volume_bcm'),
                                    F.count('*').alias('total_trips'),
                                    F.avg('production_volume_bcm').alias('avg_volume_per_trip'),
                                    F.sum('hauling_distance_m').alias('total_distance_m')
                                    )
    daily_production.write \
    .mode('overwrite') \
    .parquet(f"s3a://{MINIO_BUCKET}/gold/daily_production/dt={execution_date}/data.parquet")

    log.info(f"Gold daily production data written to 'gold/daily_production/dt={execution_date}/data.parquet'.")

    # 5. gold_equipment_performance: Calculate equipment performance metrics
    equipment_performance = df.groupBy('operation_date', 'excavator_code', 'excavator_model'
                                ).agg(
                                    F.sum('production_volume_bcm').alias('total_volume_bcm'),
                                    F.count('*').alias('total_trips'),
                                    F.avg('production_volume_bcm').alias('avg_volume_per_trip'),
                                    F.countDistinct('truck_code').alias('total_trucks_used')
                                    )
    equipment_performance.write \
    .mode('overwrite') \
    .parquet(f"s3a://{MINIO_BUCKET}/gold/equipment_performance/dt={execution_date}/data.parquet")

    log.info(f"Gold equipment performance data written to 'gold/equipment_performance/dt={execution_date}/data.parquet'.")

    # 6. gold_route_summary: Calculate route summary metrics
    route_summary = df.groupBy(
        'operation_date', 'loading_area', 'dumping_area', 'material_group'
    ).agg(
        F.sum('production_volume_bcm').alias('total_volume_bcm'),
        F.count('*').alias('total_trips'),
        F.avg('hauling_distance_m').alias('avg_hauling_distance_m'),
        F.sum('hauling_distance_m').alias('total_distance_m')
    )

    route_summary.write \
        .mode('overwrite') \
        .parquet(f"s3a://{MINIO_BUCKET}/gold/route_summary/dt={execution_date}/data.parquet")

    log.info(f"gold_route_summary saved: {route_summary.count()} rows.")

    # 7. gold_supervisor_performance: Calculate supervisor performance metrics
    supervisor_performance = df.groupBy(
        'operation_date', 'supervisor_name'
    ).agg(
        F.sum('production_volume_bcm').alias('total_volume_bcm'),
        F.count('*').alias('total_trips'),
        F.countDistinct('excavator_code').alias('total_excavators'),
        F.countDistinct('truck_code').alias('total_trucks')
    )

    supervisor_performance.write \
        .mode('overwrite') \
        .parquet(f"s3a://{MINIO_BUCKET}/gold/supervisor_performance/dt={execution_date}/data.parquet")

    log.info(f"gold_supervisor_performance saved: {supervisor_performance.count()} rows.")

    spark.stop()  # Stop the Spark session after initialization to avoid resource leaks
    log.info("Gold transformation completed.")