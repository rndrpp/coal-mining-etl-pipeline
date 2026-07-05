import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from tasks.bronze import ingest_bronze
from tasks.silver import transform_silver
from tasks.gold import transform_gold
from tasks.load_postgres import load_postgres

log = logging.getLogger(__name__)


def alert_on_failure(context):
    """Central failure hook. Wire a Slack/email/PagerDuty call here when a channel is available."""
    task_instance = context['task_instance']
    log.error(
        "Task failed: dag=%s task=%s execution_date=%s log_url=%s",
        task_instance.dag_id,
        task_instance.task_id,
        context['ds'],
        task_instance.log_url,
    )


default_args = {
    'owner': 'airflow',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': False,
    'email_on_retry': False,
    'on_failure_callback': alert_on_failure,
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