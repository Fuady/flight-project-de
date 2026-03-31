"""
flights_pipeline.py
────────────────────────────────────────────────────────────────────────────
End-to-end batch pipeline for BTS Flight Delays data.

DAG steps:
  1. download_bts_data       – Download CSV from BTS (or local test fixture)
  2. upload_raw_to_gcs       – Upload raw CSV to GCS raw bucket
  3. run_spark_job           – Submit PySpark job to Dataproc for cleaning
  4. load_to_bigquery        – Load processed Parquet into BQ staging table
  5. run_dbt_staging         – Execute dbt staging models
  6. run_dbt_mart            – Execute dbt mart models
  7. notify_success          – Log completion

Schedule: daily (runs for the previous month's data on the 5th of each month)
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocSubmitJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.dates import days_ago

# ─── Config ──────────────────────────────────────────────────────────────────

PROJECT_ID      = os.environ.get("GCP_PROJECT_ID", "your-project-id")
REGION          = os.environ.get("GCP_REGION", "us-central1")
RAW_BUCKET      = os.environ.get("GCS_RAW_BUCKET", f"{PROJECT_ID}-flights-raw")
PROCESSED_BUCKET = os.environ.get("GCS_PROCESSED_BUCKET", f"{PROJECT_ID}-flights-processed")
DATAPROC_CLUSTER = os.environ.get("DATAPROC_CLUSTER", "flights-spark-cluster")
BQ_STAGING_DATASET = "flights_staging"
BQ_STAGING_TABLE   = "raw_flights"
SPARK_JOB_FILE  = f"gs://{RAW_BUCKET}/scripts/spark_transform.py"

BTS_BASE_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present"
)

DEFAULT_ARGS = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

# ─── BQ Schema ───────────────────────────────────────────────────────────────

BQ_SCHEMA = [
    {"name": "fl_date",            "type": "DATE",    "mode": "NULLABLE"},
    {"name": "mkt_carrier",        "type": "STRING",  "mode": "NULLABLE"},
    {"name": "mkt_carrier_fl_num", "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "origin",             "type": "STRING",  "mode": "NULLABLE"},
    {"name": "dest",               "type": "STRING",  "mode": "NULLABLE"},
    {"name": "dep_delay",          "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "arr_delay",          "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "cancelled",          "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "cancellation_code",  "type": "STRING",  "mode": "NULLABLE"},
    {"name": "carrier_delay",      "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "weather_delay",      "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "nas_delay",          "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "security_delay",     "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "late_aircraft_delay","type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "actual_elapsed_time","type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "distance",           "type": "FLOAT",   "mode": "NULLABLE"},
]

# ─── Python callables ────────────────────────────────────────────────────────

def download_bts_data(**context) -> str:
    """
    Download the monthly On-Time CSV from BTS for the target year/month.
    Stores the file locally at /tmp/flights_YYYY_MM.csv.
    """
    import urllib.request
    import zipfile

    execution_date: datetime = context["execution_date"]
    # Use prior month's data
    target = execution_date.replace(day=1) - timedelta(days=1)
    year, month = target.year, target.month

    zip_name = f"On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
    url = f"{BTS_BASE_URL}_{year}_{month}.zip"
    local_zip = f"/tmp/flights_{year}_{month}.zip"
    local_csv = f"/tmp/flights_{year}_{month}.csv"

    print(f"Downloading {url} ...")
    urllib.request.urlretrieve(url, local_zip)

    with zipfile.ZipFile(local_zip, "r") as z:
        csv_files = [f for f in z.namelist() if f.endswith(".csv")]
        if not csv_files:
            raise FileNotFoundError("No CSV found inside the BTS zip")
        z.extract(csv_files[0], "/tmp/")
        import shutil
        shutil.move(f"/tmp/{csv_files[0]}", local_csv)

    context["task_instance"].xcom_push(key="local_csv", value=local_csv)
    context["task_instance"].xcom_push(key="year",      value=year)
    context["task_instance"].xcom_push(key="month",     value=month)

    print(f"Downloaded to {local_csv}")
    return local_csv


def upload_raw_to_gcs(**context) -> None:
    """Upload the local CSV to GCS raw bucket."""
    ti = context["task_instance"]
    local_csv = ti.xcom_pull(key="local_csv", task_ids="download_bts_data")
    year  = ti.xcom_pull(key="year",  task_ids="download_bts_data")
    month = ti.xcom_pull(key="month", task_ids="download_bts_data")

    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_path = f"raw/{year}/{month:02d}/flights.csv"

    gcs_hook.upload(
        bucket_name=RAW_BUCKET,
        object_name=gcs_path,
        filename=local_csv,
        mime_type="text/csv",
    )
    print(f"Uploaded to gs://{RAW_BUCKET}/{gcs_path}")

    ti.xcom_push(key="gcs_raw_path", value=f"gs://{RAW_BUCKET}/{gcs_path}")
    ti.xcom_push(key="gcs_processed_prefix",
                 value=f"gs://{PROCESSED_BUCKET}/processed/{year}/{month:02d}/")


def upload_spark_script(**context) -> None:
    """Upload the PySpark script to GCS so Dataproc can access it."""
    gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")
    gcs_hook.upload(
        bucket_name=RAW_BUCKET,
        object_name="scripts/spark_transform.py",
        filename="/opt/airflow/dags/../../spark/spark_transform.py",
        mime_type="text/plain",
    )
    print(f"Spark script uploaded to gs://{RAW_BUCKET}/scripts/spark_transform.py")


# ─── DAG Definition ──────────────────────────────────────────────────────────

with DAG(
    dag_id="flights_batch_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end batch pipeline: BTS → GCS → Spark → BigQuery → dbt",
    schedule_interval="0 6 5 * *",   # 06:00 on 5th of every month
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["flights", "batch", "gcp"],
) as dag:

    # ── Step 1: Download from BTS ─────────────────────────────────────────────
    t_download = PythonOperator(
        task_id="download_bts_data",
        python_callable=download_bts_data,
    )

    # ── Step 2: Upload raw CSV to GCS ────────────────────────────────────────
    t_upload_raw = PythonOperator(
        task_id="upload_raw_to_gcs",
        python_callable=upload_raw_to_gcs,
    )

    # ── Step 2b: Upload Spark script to GCS ──────────────────────────────────
    t_upload_script = PythonOperator(
        task_id="upload_spark_script",
        python_callable=upload_spark_script,
    )

    # ── Step 3: Run Spark on Dataproc ─────────────────────────────────────────
    t_spark = DataprocSubmitJobOperator(
        task_id="run_spark_job",
        job={
            "reference": {"project_id": PROJECT_ID},
            "placement": {"cluster_name": DATAPROC_CLUSTER},
            "pyspark_job": {
                "main_python_file_uri": SPARK_JOB_FILE,
                "args": [
                    "--input",
                    "{{ task_instance.xcom_pull(task_ids='upload_raw_to_gcs', key='gcs_raw_path') "
                    "or task_instance.xcom_pull(task_ids='download_bts_data', key='gcs_raw_path') }}",
                    "--output",
                    f"gs://{PROCESSED_BUCKET}/processed/"
                    "{{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m') }}}}/",
                ],
                "properties": {
                    "spark.executor.memory": "4g",
                    "spark.executor.cores": "2",
                },
            },
        },
        region=REGION,
        project_id=PROJECT_ID,
        gcp_conn_id="google_cloud_default",
    )

    # ── Step 4: Load Parquet → BigQuery staging ───────────────────────────────
    t_bq_load = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=PROCESSED_BUCKET,
        source_objects=[
            "processed/{{{{ macros.ds_format(ds, '%Y-%m-%d', '%Y/%m') }}}}/*.parquet"
        ],
        destination_project_dataset_table=(
            f"{PROJECT_ID}.{BQ_STAGING_DATASET}.{BQ_STAGING_TABLE}"
        ),
        source_format="PARQUET",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=BQ_SCHEMA,
        create_disposition="CREATE_IF_NEEDED",
        gcp_conn_id="google_cloud_default",
        time_partitioning={"type": "MONTH", "field": "fl_date"},
        cluster_fields=["mkt_carrier", "origin"],
    )

    # ── Step 5: dbt staging models ────────────────────────────────────────────
    t_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select staging --profiles-dir . --target prod"
        ),
        env={
            "DBT_PROJECT_ID": PROJECT_ID,
            "DBT_DATASET":    BQ_STAGING_DATASET,
        },
    )

    # ── Step 6: dbt mart models ───────────────────────────────────────────────
    t_dbt_mart = BashOperator(
        task_id="run_dbt_mart",
        bash_command=(
            "cd /opt/airflow/dbt && "
            "dbt run --select mart --profiles-dir . --target prod && "
            "dbt test --select mart --profiles-dir . --target prod"
        ),
        env={
            "DBT_PROJECT_ID": PROJECT_ID,
            "DBT_DATASET":    "flights_mart",
        },
    )

    # ── Step 7: Notify success ────────────────────────────────────────────────
    t_notify = BashOperator(
        task_id="notify_success",
        bash_command='echo "Pipeline completed successfully for {{ ds }}"',
    )

    # ── DAG dependencies ──────────────────────────────────────────────────────
    (
        t_download
        >> t_upload_raw
        >> t_upload_script
        >> t_spark
        >> t_bq_load
        >> t_dbt_staging
        >> t_dbt_mart
        >> t_notify
    )
