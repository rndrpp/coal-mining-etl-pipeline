# Coal Mining ETL Pipeline

A medallion-architecture (Bronze → Silver → Gold) ETL pipeline for coal mining ritase (hauling trip) data, orchestrated with Apache Airflow and processed with Spark.

## Architecture

```
Excel source ──▶ Bronze (MinIO, raw parquet)
                     │
                     ▼
                 Silver (MinIO, cleaned/validated parquet)
                     │
                     ▼
                  Gold (MinIO, aggregated marts: shift, daily,
                     │        equipment, route, supervisor)
                     ▼
              PostgreSQL DWH (upserted gold tables)
```

- **Orchestration**: Apache Airflow 2.8 (CeleryExecutor, Redis broker, Postgres metadata DB)
- **Processing**: PySpark 3.5 (via `hadoop-aws` for S3A access to MinIO)
- **Data lake**: MinIO (S3-compatible object storage)
- **Data warehouse**: PostgreSQL
- **DAG**: `dags/pipeline_dag.py` — `ingest_bronze >> transform_silver >> transform_gold >> load_postgres`, daily at 07:00

## Prerequisites

- Docker and Docker Compose
- A source Excel file at `data/coal_mining_data.xlsx` (not included in this repo — see `.gitignore`)

## Setup

1. Copy the environment template and fill in real values (never commit `.env`):
   ```bash
   cp .env.example .env
   ```
   Generate a Fernet key for `AIRFLOW__CORE__FERNET_KEY`:
   ```bash
   python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
   ```
   Set strong values for `DWH_PASSWORD`, `REDIS_PASSWORD`, `MINIO_ROOT_PASSWORD`, `POSTGRES_PASSWORD`, and `AIRFLOW_ADMIN_PASSWORD`.

2. Place your source data at `data/coal_mining_data.xlsx`.

3. Start the stack:
   ```bash
   docker compose up -d --build
   ```

4. Access services:
   - Airflow webserver: http://localhost:8080 (login with `AIRFLOW_ADMIN_USER` / `AIRFLOW_ADMIN_PASSWORD` from `.env`)
   - Flower (Celery monitoring): http://localhost:5555
   - MinIO console: http://localhost:9001
   - Spark master UI: http://localhost:8181
   - PostgreSQL DWH: `localhost:5432`, database `dwh`

5. Unpause and trigger the `coal_pipeline` DAG from the Airflow UI, or:
   ```bash
   docker compose exec airflow-webserver airflow dags unpause coal_pipeline
   docker compose exec airflow-webserver airflow dags trigger coal_pipeline
   ```

## Project layout

```
dags/
  pipeline_dag.py        # DAG definition, task wiring, failure alerting hook
  tasks/
    bronze.py             # Excel -> MinIO parquet (raw)
    silver.py              # Clean, dedupe, validate, standardize
    gold.py                 # Aggregations (shift/daily/equipment/route/supervisor)
    load_postgres.py         # Upsert gold tables into PostgreSQL
docker/airflow/            # Custom Airflow image (adds Java for PySpark)
init_db/                    # DWH bootstrap script (reads creds from env, not hardcoded)
.github/workflows/          # CI: lint + DAG import validation
tests/                       # DAG integrity tests
```

## Testing

```bash
pip install -r docker/airflow/requirements.txt -r requirements-dev.txt
pytest
```

## Production notes

- Rotate all default credentials in `.env` before any non-local deployment — none of the values in `.env.example` are safe to use as-is.
- `on_failure_callback` in `pipeline_dag.py` currently only logs; wire it to Slack/PagerDuty/email for real alerting.
- Consider moving secrets out of `.env`/docker-compose and into Airflow Connections or a secrets backend (Vault, AWS Secrets Manager) for production.
