"""
Airflow DAG for the DataForge pipeline.

Orchestrates the pipeline as scheduled, dependency-ordered tasks with retries.
(This is the orchestration definition; running it requires an Airflow
deployment. It documents the production task graph.)

Task graph:
    ingest -> validate -> model -> load -> optimize
"""
from datetime import datetime, timedelta

# These imports require an Airflow installation to run.
try:
    from airflow import DAG
    from airflow.operators.python import PythonOperator
    AIRFLOW_AVAILABLE = True
except ImportError:
    AIRFLOW_AVAILABLE = False


default_args = {
    "owner": "dataforge",
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": True,
}


def _ingest():
    from ingestion.ingest import load_orders  # noqa
    # In production, ingest raw data into staging.
    print("ingest: load raw orders + products")


def _validate():
    from quality.validate import validate_orders  # noqa
    print("validate: split clean vs quarantine")


def _model():
    from modeling.star_schema import build_fact_sales  # noqa
    print("model: build star schema (fact + dims)")


def _load():
    from load.incremental import upsert_fact  # noqa
    print("load: incremental upsert into warehouse")


def _optimize():
    print("optimize: partitioned write + compaction")


if AIRFLOW_AVAILABLE:
    with DAG(
        dag_id="dataforge_pipeline",
        default_args=default_args,
        description="Retail sales ETL: ingest -> validate -> model -> load -> optimize",
        schedule_interval="@daily",         # run once a day
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=["etl", "warehouse"],
    ) as dag:

        ingest = PythonOperator(task_id="ingest", python_callable=_ingest)
        validate = PythonOperator(task_id="validate", python_callable=_validate)
        model = PythonOperator(task_id="model", python_callable=_model)
        load = PythonOperator(task_id="load", python_callable=_load)
        optimize = PythonOperator(task_id="optimize", python_callable=_optimize)

        # dependency order
        ingest >> validate >> model >> load >> optimize
