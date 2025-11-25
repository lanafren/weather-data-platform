import os
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from airflow.sdk import timezone
from datetime import datetime, timedelta
from google.cloud import bigquery
import logging
import pandas as pd
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from google.oauth2 import service_account



# Configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BQ_DBT_DATASET')
METADATA_TABLE = f'{PROJECT_ID}.{DATASET_ID}.table_row_counts'

TABLES_TO_CHECK = [
    'stg_forecast',
    'stg_current',
    'int_weather_unified',
    'fact_weather'
]

credentials = service_account.Credentials.from_service_account_file(
    os.environ['GOOGLE_APPLICATION_CREDENTIALS']
)
client = bigquery.Client(project=PROJECT_ID, credentials=credentials)

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'bq_data_quality_checks',
    default_args=default_args,
    description='Monitor table row counts and freshness in BigQuery',
    schedule='30 */4 * * *',
    start_date=timezone.utcnow() - timedelta(days=1),
    catchup=False,
    tags=['data-quality', 'monitoring'],
)

def get_current_metrics_func(**context):
    client = bigquery.Client(project=PROJECT_ID)
    metrics = {}
    
    
    for table_name in TABLES_TO_CHECK:
        timestamp_fields = ['fetched_at', 'loaded_at', 'updated_at', 'created_at']
        timestamp_field = None
        
        table_ref = f'{PROJECT_ID}.{DATASET_ID}.{table_name}'
        try:
            table = client.get_table(table_ref)
            field_names = [f.name for f in table.schema]
            for ts_field in timestamp_fields:
                if ts_field in field_names:
                    timestamp_field = ts_field
                    break
        except Exception as e:
            logging.error(f"Error accessing {table_name}: {e}")
            continue

        query = f"""
        SELECT 
            '{table_name}' as table_name,
            COUNT(*) as row_count,
            {f'MAX({timestamp_field})' if timestamp_field else 'NULL'} as max_updated_at
        FROM `{PROJECT_ID}.{DATASET_ID}.{table_name}`
        """
        try:
            result = client.query(query).to_dataframe()
            max_ts = result['max_updated_at'].iloc[0]

            if timestamp_field and max_ts is not None and not pd.isna(max_ts):
                if isinstance(max_ts, pd.Timestamp):
                    max_ts = max_ts.to_pydatetime()
                # Round to nearest hour to match record_time_utc logic
                max_ts = max_ts.replace(minute=0, second=0, microsecond=0)
            else:
                max_ts = None

            metrics[table_name] = {
                'row_count': int(result['row_count'].iloc[0]),
                'max_updated_at': max_ts
            }
            logging.info(f"✓ {table_name}: {metrics[table_name]['row_count']} rows, max_ts: {metrics[table_name]['max_updated_at']}")
        except Exception as e:
            logging.error(f"Error querying {table_name}: {e}")
            raise
    
    return metrics

def validate_checks(**context):
    client = bigquery.Client(project=PROJECT_ID)
    failures = []
    current_metrics = context['ti'].xcom_pull(task_ids='get_current_metrics')
    
    query = f"""
    SELECT 
        table_name,
        row_count as previous_row_count,
        max_updated_at as previous_max_updated_at
    FROM `{METADATA_TABLE}`
    WHERE check_timestamp = (
        SELECT MAX(check_timestamp) FROM `{METADATA_TABLE}`
    )
    """
    try:
        previous_metrics = client.query(query).to_dataframe()
        previous_dict = previous_metrics.set_index('table_name').to_dict('index')
    except Exception as e:
        logging.warning(f"No previous metrics: {e}")
        previous_dict = {}

    for table_name, current in current_metrics.items():
        if table_name not in previous_dict:
            logging.info(f"No previous data for {table_name}, skipping validation")
            continue

        previous = previous_dict[table_name]
        current_count = current['row_count']
        previous_count = previous['previous_row_count']
        current_max_ts = current['max_updated_at']
        previous_max_ts = previous['previous_max_updated_at']

        # Convert to datetime if needed
        if isinstance(current_max_ts, pd.Timestamp):
            current_max_ts = current_max_ts.to_pydatetime()
        if isinstance(previous_max_ts, pd.Timestamp):
            previous_max_ts = previous_max_ts.to_pydatetime()
        if current_max_ts and current_max_ts.tzinfo is None:
            current_max_ts = current_max_ts.replace(tzinfo=timezone.utc)
        if previous_max_ts and previous_max_ts.tzinfo is None:
            previous_max_ts = previous_max_ts.replace(tzinfo=timezone.utc)

        # Row count decrease check
        if current_count < previous_count:
            failures.append(
                f"❌ {table_name}: Row count decreased significantly "
                f"({previous_count} → {current_count}, "
                f"{((current_count - previous_count) / previous_count * 100):.1f}%)"
            )

        # Backwards timestamp check — skip fact_weather loaded_at
        if table_name != 'fact_weather' and current_max_ts and previous_max_ts:
            if current_max_ts < previous_max_ts:
                failures.append(
                    f"❌ {table_name}: max_updated_at went backwards "
                    f"({previous_max_ts} → {current_max_ts})"
                )

        # Freshness check
        if table_name in TABLES_TO_CHECK and current_max_ts:
            now = datetime.now(timezone.utc)
            hours_old = (now - current_max_ts).total_seconds() / 3600
            # Allow slightly longer window for fact_weather due to rounding
            freshness_limit = 6 if table_name != 'fact_weather' else 9
            if hours_old > freshness_limit:
                failures.append(
                    f"⚠️ {table_name}: Data is stale "
                    f"(rounded max_updated_at is {hours_old:.1f} hours old)"
                )

    if failures:
        error_msg = "Data quality checks failed:\n" + "\n".join(failures)
        logging.error(error_msg)
        raise AirflowException(error_msg)
    else:
        logging.info("✅ All data quality checks passed!")

# Tasks
get_current_metrics = PythonOperator(
    task_id='get_current_metrics',
    python_callable=get_current_metrics_func,
    dag=dag,
)

validate_checks_task = PythonOperator(
    task_id='validate_checks',
    python_callable=validate_checks,
    dag=dag,
)

update_metadata_query = f"""
INSERT INTO `{METADATA_TABLE}` (table_name, row_count, max_updated_at, check_timestamp)
SELECT 
    table_name,
    row_count,
    max_updated_at,
    CURRENT_TIMESTAMP() as check_timestamp
FROM (
    SELECT 'stg_forecast' as table_name, COUNT(*) as row_count, MAX(fetched_at) as max_updated_at FROM `{PROJECT_ID}.{DATASET_ID}.stg_forecast`
    UNION ALL
    SELECT 'stg_current', COUNT(*), MAX(fetched_at) FROM `{PROJECT_ID}.{DATASET_ID}.stg_current`
    UNION ALL
    SELECT 'int_weather_unified', COUNT(*), MAX(loaded_at) FROM `{PROJECT_ID}.{DATASET_ID}.int_weather_unified`
    UNION ALL
    SELECT 'fact_weather', COUNT(*), MAX(loaded_at) FROM `{PROJECT_ID}.{DATASET_ID}.fact_weather`
    UNION ALL
    SELECT 'dim_datetime', COUNT(*), NULL FROM `{PROJECT_ID}.{DATASET_ID}.dim_datetime`
)
"""

def update_metadata_func(**context):
    
    credentials = service_account.Credentials.from_service_account_file(
        os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    )
    client = bigquery.Client(project=PROJECT_ID, credentials=credentials)
    
    query_job = client.query(update_metadata_query)
    query_job.result()  # wait for completion
    logging.info("✅ Metadata table updated")

update_metadata_task = PythonOperator(
    task_id='update_metadata',
    python_callable=update_metadata_func,
    dag=dag,
)

get_current_metrics >> validate_checks_task >> update_metadata_task
