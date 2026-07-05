from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks.bronze import ingest_bronze
from tasks.silver import transform_silver
from tasks.gold import transform_gold
from tasks.load_postgres import load_postgres



default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
}

with DAG(
    dag_id='coal_pipeline',
    default_args=default_args,
    description='Coal mining ritase pipeline - Bronze to PostgreSQL',
    schedule_interval='0 7 * * *',
    start_date=datetime(2026, 6, 1),
    catchup=False,
    tags=['coal', 'pipeline', 'medallion'],
) as dag:

    bronze_task = PythonOperator(
        task_id='ingest_bronze',
        python_callable=ingest_bronze,
    )

    silver_task = PythonOperator(
        task_id='transform_silver',
        python_callable=transform_silver,
    )

    gold_task = PythonOperator(
        task_id='transform_gold',
        python_callable=transform_gold,
    )

    load_postgres_task = PythonOperator(
        task_id='load_postgres',
        python_callable=load_postgres,
    )

    bronze_task >> silver_task >> gold_task >> load_postgres_task