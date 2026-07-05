import os

from airflow.models import DagBag

DAGS_FOLDER = os.path.join(os.path.dirname(__file__), "..", "dags")


def _load_dagbag():
    return DagBag(dag_folder=DAGS_FOLDER, include_examples=False)


def test_no_import_errors():
    dagbag = _load_dagbag()
    assert not dagbag.import_errors, f"DAG import errors: {dagbag.import_errors}"


def test_pipeline_dag_loaded():
    dagbag = _load_dagbag()
    dag = dagbag.get_dag("coal_pipeline")
    assert dag is not None


def test_pipeline_dag_task_order():
    dagbag = _load_dagbag()
    dag = dagbag.get_dag("coal_pipeline")

    task_ids = [task.task_id for task in dag.tasks]
    assert task_ids == [
        "ingest_bronze",
        "transform_silver",
        "transform_gold",
        "load_postgres",
    ]

    bronze = dag.get_task("ingest_bronze")
    silver = dag.get_task("transform_silver")
    gold = dag.get_task("transform_gold")
    load = dag.get_task("load_postgres")

    assert silver.task_id in [t.task_id for t in bronze.downstream_list]
    assert gold.task_id in [t.task_id for t in silver.downstream_list]
    assert load.task_id in [t.task_id for t in gold.downstream_list]


def test_pipeline_dag_has_retries_and_failure_callback():
    dagbag = _load_dagbag()
    dag = dagbag.get_dag("coal_pipeline")

    assert dag.default_args.get("retries", 0) > 0
    assert dag.default_args.get("on_failure_callback") is not None
