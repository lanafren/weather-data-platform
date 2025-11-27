import os
import json
from datetime import datetime, timedelta
from airflow.utils import timezone # type: ignore
from airflow import DAG
from airflow.operators.python import PythonOperator # type: ignore
from airflow.utils.log.logging_mixin import LoggingMixin # type: ignore 
from airflow.utils.email import send_email # type: ignore
from google.cloud import storage
from google.cloud import bigquery
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator # type: ignore
import requests # type: ignore

# ENV
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
REGION = os.getenv('GCP_REGION')
BUCKET = os.getenv('GCS_BUCKET')
BQ_DATASET = os.getenv('BQ_RAW_DATASET')
API_KEY = os.getenv('OWM_API')
LAT = os.getenv('BERLIN_LAT')
LON = os.getenv('BERLIN_LON')
EMAIL=os.getenv('EMAIL')

# CONSTANTS
CURRENT_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.current_raw"
FORECAST_TABLE_ID = f"{PROJECT_ID}.{BQ_DATASET}.forecast_raw"

default_args = {
    'owner': 'lana',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

log = LoggingMixin().log

# VALIDATION
def validate_forecast_record(r):
    """Ensure essential top-level keys exist in forecast record."""
    missing = [k for k in ["fetched_at", "source", "data"] if k not in r]
    if missing:
        raise ValueError(f"Missing keys in forecast record: {missing}")


# HELPER FUNCTIONS
def file_exists_in_gcs(bucket_name, object_name):
    """Check if a file exists in GCS."""
    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(object_name)
        exists = blob.exists()
        if exists:
            log.info(f"File gs://{bucket_name}/{object_name} already exists in GCS")
        return exists
    except Exception as e:
        log.info(f"Error checking GCS file existence: {e}")
        return False


def record_exists_in_bq(table_id, fetched_at_iso, source):
    """Check if a record with this fetched_at and source already exists in BigQuery."""
    try:
        client = bigquery.Client(project=PROJECT_ID)
        query = f"""
            SELECT COUNT(*) as count
            FROM `{table_id}`
            WHERE fetched_at = TIMESTAMP('{fetched_at_iso}')
            AND source = '{source}'
        """
        result = client.query(query).result()
        count = next(result).count
        exists = count > 0
        if exists:
            log.info(f"Record already exists in BigQuery table {table_id}")
        return exists
    except Exception as e:
        log.info(f"Error checking BigQuery record existence: {e}")
        # If table doesn't exist yet, that's fine - return False
        if "Not found: Table" in str(e):
            log.info(f"📊 Table {table_id} doesn't exist yet, will be created on first load")
            return False
        return False

# TASKS
def fetch_current_to_ndjson(**context):
    """Fetch current weather data from OpenWeatherMap API and save as NDJSON."""
    ds_nodash = context["ds_nodash"]
    
    # Round to the nearest 4-hour interval (0, 4, 8, 12, 16, 20)
    logical_date = context["logical_date"]
    hour = (logical_date.hour // 4) * 4  # Round down to nearest 4-hour mark
    rounded_date = logical_date.replace(hour=hour, minute=0, second=0, microsecond=0)

    hour_str = f"{hour:02d}"  # Format as 2-digit hour
    gcs_object = f"current_{ds_nodash}_{hour_str}.ndjson"

    # Use rounded_date for consistency
    fetched_at_iso = rounded_date.replace(tzinfo=timezone.utc).isoformat()
    
    # Check if file already exists in GCS
    if file_exists_in_gcs(BUCKET, gcs_object):
        log.info(f"Skipping API fetch - file already exists")
        ti = context['ti']
        ti.xcom_push(key='skip_fetch', value=True)
        ti.xcom_push(key='gcs_object', value=gcs_object)
        return
    
    # Fetch from API
    log.info(f"Fetching current weather data from API...")
    url = "http://api.openweathermap.org/data/2.5/weather"
    params = {'lat': LAT, 'lon': LON, 'appid': API_KEY, 'units': 'metric'}
    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()

    payload = response.json()
    
    record = {
        'fetched_at': fetched_at_iso,
        'source': 'openweathermap.current',
        'data': payload,
    }

    local_path = f"/tmp/current_{ds_nodash}_{hour_str}.ndjson"

    with open(local_path, 'w', encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    ti = context['ti']
    ti.xcom_push(key='local_path', value=local_path)
    ti.xcom_push(key='gcs_object', value=gcs_object)
    ti.xcom_push(key='skip_fetch', value=False)
    log.info(f"Created local file: {local_path}")


def fetch_forecast_to_ndjson(**context):
    """Fetch forecast weather data from OpenWeatherMap API and save as NDJSON."""
    ds_nodash = context["ds_nodash"]
    
    # Round to the nearest 4-hour interval (0, 4, 8, 12, 16, 20)
    logical_date = context["logical_date"]
    hour = (logical_date.hour // 4) * 4  # Round down to nearest 4-hour mark
    rounded_date = logical_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    
    hour_str = f"{hour:02d}"  # Format as 2-digit hour
    gcs_object = f"forecast_{ds_nodash}_{hour_str}.ndjson"
    
    # Use rounded_date for consistency
    fetched_at_iso = rounded_date.replace(tzinfo=timezone.utc).isoformat()
    
    # Check if file already exists in GCS
    if file_exists_in_gcs(BUCKET, gcs_object):
        log.info(f"Skipping API fetch - file already exists")
        ti = context['ti']
        ti.xcom_push(key='skip_fetch', value=True)
        ti.xcom_push(key='gcs_object', value=gcs_object)
        return
    
    # Fetch from API
    log.info(f"Fetching forecast weather data from API...")
    url = "https://api.openweathermap.org/data/2.5/forecast"
    params = {"lat": LAT, "lon": LON, "appid": API_KEY, "units": "metric"}
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    payload = r.json()

    now_dt = context["logical_date"].replace(tzinfo=timezone.utc)
    ds_date = context["logical_date"].date()

    # Convert each forecast entry to (datetime, item)
    items = [(datetime.fromtimestamp(it["dt"], tz=timezone.utc), it) for it in payload.get("list", [])]

    same_date = [(t, it) for (t, it) in items if t.date() == ds_date]
    candidates = same_date if same_date else items
    
    target_t, closest = min(candidates, key=lambda x: abs(x[0] - now_dt))

    record = {
        "fetched_at": fetched_at_iso,
        "source": "openweathermap.forecast_single",
        "target_time": target_t.isoformat(),
        "data": closest,
        "hours_diff": round(abs((target_t - now_dt).total_seconds()) / 3600, 2),
        "ds": context["ds"],
    }

    validate_forecast_record(record)

    local_path = f"/tmp/forecast_{ds_nodash}_{hour_str}.ndjson"

    with open(local_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")

    ti = context["ti"]
    ti.xcom_push(key="local_path", value=local_path)
    ti.xcom_push(key="gcs_object", value=gcs_object)
    ti.xcom_push(key='skip_fetch', value=False)
    log.info(f"✅ Created local file: {local_path}")


def upload_to_gcs(**context):
    """Upload NDJSON files from local /tmp to GCS, only if they were fetched."""
    ti = context['ti']

    def safe_xcom_pull(key, task_id):
        v = ti.xcom_pull(key=key, task_ids=task_id)
        if v is None:
            log.info(f"⚠️ XCom key '{key}' missing from {task_id}")
        return v

    current_data = {
        'local_path': safe_xcom_pull('local_path', 'fetch_current_to_ndjson'),
        'gcs_object': safe_xcom_pull('gcs_object', 'fetch_current_to_ndjson'),
        'skip_fetch': safe_xcom_pull('skip_fetch', 'fetch_current_to_ndjson') or False,
    }

    forecast_data = {
        'local_path': safe_xcom_pull('local_path', 'fetch_forecast_to_ndjson'),
        'gcs_object': safe_xcom_pull('gcs_object', 'fetch_forecast_to_ndjson'),
        'skip_fetch': safe_xcom_pull('skip_fetch', 'fetch_forecast_to_ndjson') or False,
    }

    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(BUCKET)

    # Upload current weather if it was fetched
    if not current_data['skip_fetch'] and current_data['local_path']:
        blob = bucket.blob(current_data['gcs_object'])
        blob.upload_from_filename(current_data['local_path'])
        log.info(f"Uploaded {current_data['local_path']} → gs://{BUCKET}/{current_data['gcs_object']}")
    else:
        log.info(f"Skipping upload for current weather - already exists in GCS")

    # Upload forecast if it was fetched
    if not forecast_data['skip_fetch'] and forecast_data['local_path']:
        blob = bucket.blob(forecast_data['gcs_object'])
        blob.upload_from_filename(forecast_data['local_path'])
        log.info(f"Uploaded {forecast_data['local_path']} → gs://{BUCKET}/{forecast_data['gcs_object']}")
    else:
        log.info(f"Skipping upload for forecast - already exists in GCS")


def check_bq_current_exists(**context):
    """Check if current weather record already exists in BigQuery."""
    # Round to the nearest 4-hour interval
    logical_date = context["logical_date"]
    hour = (logical_date.hour // 4) * 4
    rounded_date = logical_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    
    fetched_at_iso = rounded_date.replace(tzinfo=timezone.utc).isoformat()
    source = 'openweathermap.current'
    
    exists = record_exists_in_bq(CURRENT_TABLE_ID, fetched_at_iso, source)
    
    ti = context['ti']
    ti.xcom_push(key='skip_bq_load', value=exists)
    
    if exists:
        log.info(f"Will skip BigQuery load - record already exists")
    else:
        log.info(f"Will proceed with BigQuery load")


def check_bq_forecast_exists(**context):
    """Check if forecast weather record already exists in BigQuery."""
    # Round to the nearest 4-hour interval
    logical_date = context["logical_date"]
    hour = (logical_date.hour // 4) * 4
    rounded_date = logical_date.replace(hour=hour, minute=0, second=0, microsecond=0)
    fetched_at_iso = rounded_date.replace(tzinfo=timezone.utc).isoformat()
    source = 'openweathermap.forecast_single'
    
    exists = record_exists_in_bq(FORECAST_TABLE_ID, fetched_at_iso, source)
    
    ti = context['ti']
    ti.xcom_push(key='skip_bq_load', value=exists)
    
    if exists:
        log.info(f"Will skip BigQuery load - record already exists")
    else:
        log.info(f"Will proceed with BigQuery load")


def conditional_bq_load_current(**context):
    """Load to BigQuery only if record doesn't exist."""
    ti = context['ti']
    skip_load = ti.xcom_pull(key='skip_bq_load', task_ids='check_bq_current_exists')
    
    if skip_load:
        log.info(f"Skipping BigQuery load - record already exists")
        return
    
    log.info(f"Loading current weather data to BigQuery...")
    
    ds_nodash = context["ds_nodash"]
    logical_date = context["logical_date"]
    hour = (logical_date.hour // 4) * 4
    hour_str = f"{hour:02d}"
    gcs_object = f"current_{ds_nodash}_{hour_str}.ndjson"
    
    client = bigquery.Client(project=PROJECT_ID)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data", "JSON", mode="REQUIRED"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fetched_at",
        ),
    )
    
    uri = f"gs://{BUCKET}/{gcs_object}"
    load_job = client.load_table_from_uri(uri, CURRENT_TABLE_ID, job_config=job_config)
    load_job.result()  # Wait for job to complete
    log.info(f"Loaded data to {CURRENT_TABLE_ID}")


def conditional_bq_load_forecast(**context):
    """Load to BigQuery only if record doesn't exist."""
    ti = context['ti']
    skip_load = ti.xcom_pull(key='skip_bq_load', task_ids='check_bq_forecast_exists')
    
    if skip_load:
        log.info(f"Skipping BigQuery load - record already exists")
        return
    
    log.info(f"Loading forecast weather data to BigQuery...")
    
    ds_nodash = context["ds_nodash"]
    # FIX: Use the same rounding logic as fetch tasks
    logical_date = context["logical_date"]
    hour = (logical_date.hour // 4) * 4
    hour_str = f"{hour:02d}"
    gcs_object = f"forecast_{ds_nodash}_{hour_str}.ndjson"
    
    client = bigquery.Client(project=PROJECT_ID)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("fetched_at", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("source", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("target_time", "TIMESTAMP", mode="NULLABLE"),
            bigquery.SchemaField("hours_diff", "FLOAT", mode="NULLABLE"),
            bigquery.SchemaField("ds", "STRING", mode="NULLABLE"),
            bigquery.SchemaField("data", "JSON", mode="REQUIRED"),
        ],
        time_partitioning=bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field="fetched_at",
        ),
        ignore_unknown_values=True,
    )
    
    uri = f"gs://{BUCKET}/{gcs_object}"
    load_job = client.load_table_from_uri(uri, FORECAST_TABLE_ID, job_config=job_config)
    load_job.result()  # Wait for job to complete
    log.info(f"Loaded data to {FORECAST_TABLE_ID}")

def notify_sla_miss(dag, task_list, blocking_task_list, slas, blocking_tis):
    """
    Sends an email when a task SLA is missed.
    """
    log = LoggingMixin().log
    failed_tasks = [ti.task_id for ti in blocking_tis]
    log.error(f"SLA missed for tasks: {', '.join(failed_tasks)}")

    if EMAIL:  # make sure EMAIL is set in env
        subject = f"[Airflow] SLA Missed in DAG: {dag.dag_id}"
        body = f"""
        The following tasks missed their SLA in DAG: {dag.dag_id} ({dag.start_date}):
        {', '.join(failed_tasks)}

        Check Airflow UI for details: {os.getenv('AIRFLOW__WEBSERVER__BASE_URL', 'http://localhost:8080')}
        """
        try:
            send_email(to=EMAIL, subject=subject, html_content=body)
            log.info(f"SLA notification sent to {EMAIL}")
        except Exception as e:
            log.error(f"Failed to send SLA email: {e}")


# ─── DAG DEFINITION ──────────────────────────────────────────────────
with DAG(
    dag_id='owm_batch_bq',
    start_date=timezone.utcnow() - timedelta(days=1),
    description='Fetch weather data from OWM and load to BigQuery',
    schedule='0 */4 * * *',  # every 4 hours
    default_args=default_args,
    max_active_runs=1,
    tags=['owm', 'bigquery'],
    sla_miss_callback=notify_sla_miss,
) as dag:

    fetch_current = PythonOperator(
        task_id='fetch_current_to_ndjson',
        python_callable=fetch_current_to_ndjson,
        sla=timedelta(minutes=15)
    )

    fetch_forecast = PythonOperator(
        task_id='fetch_forecast_to_ndjson',
        python_callable=fetch_forecast_to_ndjson,
        sla=timedelta(minutes=15)
    )

    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs,
        sla=timedelta(minutes=15)
    )

    check_current_bq = PythonOperator(
        task_id='check_bq_current_exists',
        python_callable=check_bq_current_exists,
        sla=timedelta(minutes=15)
    )

    check_forecast_bq = PythonOperator(
        task_id='check_bq_forecast_exists',
        python_callable=check_bq_forecast_exists,
        sla=timedelta(minutes=15)
    )

    load_current_bq = PythonOperator(
        task_id='conditional_bq_load_current',
        python_callable=conditional_bq_load_current,
        sla=timedelta(minutes=15)
    )

    load_forecast_bq = PythonOperator(
        task_id='conditional_bq_load_forecast',
        python_callable=conditional_bq_load_forecast,
        sla=timedelta(minutes=15)
    )

    # Task dependencies
    [fetch_current, fetch_forecast] >> upload_task
    upload_task >> check_current_bq >> load_current_bq
    upload_task >> check_forecast_bq >> load_forecast_bq