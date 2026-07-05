import os
import logging
from pyspark.sql import SparkSession
import psycopg2
from psycopg2.extras import execute_values

log = logging.getLogger(__name__)

# MinIO config
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000").replace("http://", "")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
MINIO_BUCKET = os.getenv("MINIO_BUCKET", "coal-lake")

# PostgreSQL config
DWH_HOST = os.getenv("DWH_HOST", "postgres")
DWH_PORT = os.getenv("DWH_PORT", "5432")
DWH_DB = os.getenv("DWH_DB", "dwh")
DWH_USER = os.getenv("DWH_USER", "dwh")
DWH_PASSWORD = os.getenv("DWH_PASSWORD")

def load_postgres(**context):
    log.info("Starting load to PostgreSQL...")

    execution_date = context['ds']  # format: YYYY-MM-DD

    # 1. Connect to PostgreSQL
    conn = psycopg2.connect(
        host=DWH_HOST,
        port=DWH_PORT,
        dbname=DWH_DB,
        user=DWH_USER,
        password=DWH_PASSWORD
    )
    conn.autocommit = False
    cursor = conn.cursor()

    # 2. Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS gold_shift_production (
            operation_date DATE,
            work_shift VARCHAR(50),
            material_group VARCHAR(100),
            total_volume_bcm DECIMAL(10, 2),
            total_trips INTEGER,
            avg_volume_per_trip DECIMAL(10, 2),
            total_distance_m DECIMAL(10, 2),
            PRIMARY KEY (operation_date, work_shift, material_group)
        );
         CREATE TABLE IF NOT EXISTS gold_daily_production (
            operation_date DATE,
            material_group VARCHAR(100),
            total_volume_bcm DECIMAL(10, 2),
            total_trips INTEGER,
            avg_volume_per_trip DECIMAL(10, 2),
            total_distance_m DECIMAL(10, 2),
            PRIMARY KEY (operation_date, material_group)
        );
        CREATE TABLE IF NOT EXISTS gold_equipment_performance (
            operation_date DATE,
            excavator_code VARCHAR(50),
            excavator_model VARCHAR(100),
            total_volume_bcm DECIMAL(10, 2),
            total_trips INTEGER,
            avg_volume_per_trip DECIMAL(10, 2),
            total_trucks_used INTEGER,
            PRIMARY KEY (operation_date, excavator_code)
        );
        CREATE TABLE IF NOT EXISTS gold_route_summary (
        operation_date        DATE,
        loading_area          VARCHAR(50),
        dumping_area          VARCHAR(50),
        material_group        VARCHAR(50),
        total_volume_bcm      DECIMAL(10,2),
        total_trips           INTEGER,
        avg_hauling_distance_m DECIMAL(10,2),
        total_distance_m      BIGINT,
        PRIMARY KEY (operation_date, loading_area, dumping_area, material_group)
        );

        CREATE TABLE IF NOT EXISTS gold_supervisor_performance (
        operation_date   DATE,
        supervisor_name  VARCHAR(100),
        total_volume_bcm DECIMAL(10,2),
        total_trips      INTEGER,
        total_excavators INTEGER,
        total_trucks     INTEGER,
        PRIMARY KEY (operation_date, supervisor_name)
        );
    """)
    conn.commit()
    log.info("PostgreSQL tables ensured.")

    # 3. Initialize Spark session & read Gold data from MinIO
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName("coal_load_postgres") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.endpoint", f"http://{MINIO_ENDPOINT}") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()
    
    df_shift = spark.read.parquet(f"s3a://{MINIO_BUCKET}/gold/shift_production/dt={execution_date}/data.parquet")
    df_daily = spark.read.parquet(f"s3a://{MINIO_BUCKET}/gold/daily_production/dt={execution_date}/data.parquet")
    df_equipment = spark.read.parquet(f"s3a://{MINIO_BUCKET}/gold/equipment_performance/dt={execution_date}/data.parquet")
    df_route = spark.read.parquet(f"s3a://{MINIO_BUCKET}/gold/route_summary/dt={execution_date}/data.parquet")
    df_supervisor = spark.read.parquet(f"s3a://{MINIO_BUCKET}/gold/supervisor_performance/dt={execution_date}/data.parquet")


    log.info(f"Read Gold: shift={df_shift.count()}, daily={df_daily.count()}, equipment={df_equipment.count()}, route={df_route.count()}, supervisor={df_supervisor.count()} rows.")
    
    # 4. Upsert data into PostgreSQL
    def upsert(df, table, conflict_cols, update_cols):
        rows = df.collect()
        if not rows:
            log.info(f"No data to upsert for table {table}.")
            return
        
        cols = df.columns
        values = [tuple(row) for row in rows]

        conflict = ', '.join(conflict_cols)
        update = ', '.join([f"{col} = EXCLUDED.{col}" for col in update_cols])

        sql = f"""
            INSERT INTO {table} ({', '.join(cols)})
            VALUES %s
            ON CONFLICT ({conflict}) DO UPDATE SET {update};
        """
        execute_values(cursor, sql, values)
        conn.commit()
        log.info(f"Upserted {len(values)} records into {table}.")
    
    upsert(df_shift, 'gold_shift_production', 
           conflict_cols=['operation_date', 'work_shift', 'material_group'], 
           update_cols=['total_volume_bcm', 'total_trips', 'avg_volume_per_trip', 'total_distance_m'])
    upsert(df_daily, 'gold_daily_production', 
           conflict_cols=['operation_date', 'material_group'], 
           update_cols=['total_volume_bcm', 'total_trips', 'avg_volume_per_trip', 'total_distance_m'])
    upsert(df_equipment, 'gold_equipment_performance', 
           conflict_cols=['operation_date', 'excavator_code'], 
           update_cols=['total_volume_bcm', 'total_trips', 'avg_volume_per_trip', 'total_trucks_used'])
    upsert(df_route, 'gold_route_summary',
       conflict_cols=['operation_date', 'loading_area', 'dumping_area', 'material_group'],
       update_cols=['total_volume_bcm', 'total_trips', 'avg_hauling_distance_m', 'total_distance_m'])
    upsert(df_supervisor, 'gold_supervisor_performance',
        conflict_cols=['operation_date', 'supervisor_name'],
        update_cols=['total_volume_bcm', 'total_trips', 'total_excavators', 'total_trucks'])
    
    
    spark.stop()
    cursor.close()
    conn.close()
    log.info("Load to PostgreSQL complete.")