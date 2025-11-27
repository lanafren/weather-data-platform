import os
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator # type: ignore
from airflow.providers.standard.operators.python import PythonOperator # type: ignore
from airflow.exceptions import AirflowException # type: ignore
from airflow.sdk import timezone # type: ignore
from datetime import datetime, timedelta
from google.cloud import bigquery
import logging
import pandas as pd # type: ignore

# Configuration
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('BQ_DBT_DATASET')
METADATA_TABLE = f'{PROJECT_ID}.{DATASET_ID}.table_row_counts'

TABLES_TO_CHECK = [
    'stg_forecast',        # Source data - critical to monitor
    'stg_current',         # Source data - critical to monitor
    'int_weather_unified', # Integration layer - if this works, marts should work
    'fact_weather'         # Final output - verify end result
]

# Optional: Tables to check only row counts (no freshness checks)
# dim_datetime grows with new timestamps from int_weather_unified
TABLES_ROW_COUNT_ONLY = [
    'dim_datetime'
]

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
    schedule='30 */4 * * *',  # 30 minutes after ingestion (which runs at :00)
    start_date=timezone.utcnow() - timedelta(days=1),
    catchup=False,
    tags=['data-quality', 'monitoring'],
)

def validate_checks(**context):
    """
    Validate data quality checks by comparing current metrics with previous run.
    Raises AirflowException if any check fails.
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    failures = []
    
    # Get current metrics from XCom
    current_metrics = context['ti'].xcom_pull(task_ids='get_current_metrics')
    
    # Get previous metrics from metadata table
    query = f"""
    SELECT 
        table_name,
        row_count as previous_row_count,
        max_updated_at as previous_max_updated_at
    FROM `{METADATA_TABLE}`
    WHERE check_timestamp = (
        SELECT MAX(check_timestamp) 
        FROM `{METADATA_TABLE}`
    )
    """
    
    try:
        previous_metrics = client.query(query).to_dataframe()
        previous_dict = previous_metrics.set_index('table_name').to_dict('index')
    except Exception as e:
        logging.warning(f"Could not retrieve previous metrics: {e}. This might be the first run.")
        previous_dict = {}
    
    # Compare metrics
    for table_name, current in current_metrics.items():
        if table_name not in previous_dict:
            logging.info(f"No previous data for {table_name}, skipping validation")
            continue
            
        previous = previous_dict[table_name]
        current_count = current['row_count']
        previous_count = previous['previous_row_count']
        current_max_ts = current['max_updated_at']
        previous_max_ts = previous['previous_max_updated_at']
        
        # Convert pandas Timestamp to Python datetime if needed
        if isinstance(current_max_ts, pd.Timestamp):
            current_max_ts = current_max_ts.to_pydatetime()
        if isinstance(previous_max_ts, pd.Timestamp):
            previous_max_ts = previous_max_ts.to_pydatetime()
        
        # Handle timezone-naive datetimes
        if current_max_ts and current_max_ts.tzinfo is None:
            current_max_ts = current_max_ts.replace(tzinfo=timezone.utc)
        if previous_max_ts and previous_max_ts.tzinfo is None:
            previous_max_ts = previous_max_ts.replace(tzinfo=timezone.utc)
        
        # Check 1: Row count should not decrease (or decrease by more than 5%)
        if current_count < previous_count * 0.95:
            failures.append(
                f"❌ {table_name}: Row count decreased significantly "
                f"({previous_count} → {current_count}, "
                f"{((current_count - previous_count) / previous_count * 100):.1f}%)"
            )
        
        # Check 2: max(updated_at) should not decrease (only if timestamp exists)
        if current_max_ts and previous_max_ts:
            if current_max_ts < previous_max_ts:
                failures.append(
                    f"❌ {table_name}: max_updated_at went backwards "
                    f"({previous_max_ts} → {current_max_ts})"
                )
        
        # Check 3: Data freshness - max_updated_at should be recent (within 6 hours)
        # Only check for tables in TABLES_TO_CHECK
        # Adjusted to 6 hours to account for 4-hour ingestion schedule + buffer
        if table_name in TABLES_TO_CHECK and current_max_ts:
            now = datetime.now(timezone.utc)
            hours_old = (now - current_max_ts).total_seconds() / 3600
            if hours_old > 6:
                failures.append(
                    f"⚠️ {table_name}: Data is stale "
                    f"(max_updated_at is {hours_old:.1f} hours old)"
                )
    
    # Raise exception if any failures
    if failures:
        error_msg = "Data quality checks failed:\n" + "\n".join(failures)
        logging.error(error_msg)
        raise AirflowException(error_msg)
    else:
        logging.info("✅ All data quality checks passed!")

def get_current_metrics_func(**context):
    """
    Query current row counts and max timestamps for all tables.
    For staging tables: use fetched_at
    For transformed tables: use record_time_utc (the actual data timestamp)
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    metrics = {}
    
    # Map tables to their appropriate timestamp fields
    timestamp_field_map = {
        'stg_forecast': 'fetched_at',
        'stg_current': 'fetched_at',
        'int_weather_unified': 'record_time_utc',  # Use data timestamp, not loaded_at
        'fact_weather': 'record_time_utc',         # Use data timestamp, not loaded_at
        'dim_datetime': None                       # No timestamp check needed
    }
    
    for table_name in TABLES_TO_CHECK:
        timestamp_field = timestamp_field_map.get(table_name)
        
        table_ref = f'{PROJECT_ID}.{DATASET_ID}.{table_name}'
        
        # Query metrics
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
            
            # Convert pandas Timestamp to Python datetime if needed
            if timestamp_field and max_ts is not None and not pd.isna(max_ts):
                if isinstance(max_ts, pd.Timestamp):
                    max_ts = max_ts.to_pydatetime()
            else:
                max_ts = None
            
            metrics[table_name] = {
                'row_count': int(result['row_count'].iloc[0]),
                'max_updated_at': max_ts
            }
            logging.info(f"✓ {table_name}: {metrics[table_name]['row_count']} rows, "
                        f"max_ts: {metrics[table_name]['max_updated_at']}")
        except Exception as e:
            logging.error(f"Error querying {table_name}: {e}")
            raise
    
    return metrics

# Task 1: Get current metrics
get_current_metrics = PythonOperator(
    task_id='get_current_metrics',
    python_callable=get_current_metrics_func,
    dag=dag,
)

# Task 2: Validate checks
validate_checks_task = PythonOperator(
    task_id='validate_checks',
    python_callable=validate_checks,
    dag=dag,
)

# Task 3: Update metadata table
update_metadata_query = f"""
INSERT INTO `{METADATA_TABLE}` (table_name, row_count, max_updated_at, check_timestamp)
SELECT 
    table_name,
    row_count,
    max_updated_at,
    CURRENT_TIMESTAMP() as check_timestamp
FROM (
    SELECT 'stg_forecast' as table_name, COUNT(*) as row_count, 
           MAX(fetched_at) as max_updated_at 
    FROM `{PROJECT_ID}.{DATASET_ID}.stg_forecast`
    
    UNION ALL
    
    SELECT 'stg_current' as table_name, COUNT(*) as row_count, 
           MAX(fetched_at) as max_updated_at 
    FROM `{PROJECT_ID}.{DATASET_ID}.stg_current`
    
    UNION ALL
    
    SELECT 'int_weather_unified' as table_name, COUNT(*) as row_count, 
           CAST(MAX(record_time_utc) AS TIMESTAMP) as max_updated_at 
    FROM `{PROJECT_ID}.{DATASET_ID}.int_weather_unified`
    
    UNION ALL
    
    SELECT 'fact_weather' as table_name, COUNT(*) as row_count, 
           CAST(MAX(record_time_utc) AS TIMESTAMP) as max_updated_at 
    FROM `{PROJECT_ID}.{DATASET_ID}.fact_weather`
)
"""

update_metadata = BigQueryInsertJobOperator(
    task_id='update_metadata',
    configuration={
        "query": {
            "query": update_metadata_query,
            "useLegacySql": False,
        }
    },
    location='US',
    dag=dag,
)

# Set task dependencies
get_current_metrics >> validate_checks_task >> update_metadata