import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator  # Fixed
from airflow.utils.dates import days_ago
from airflow_dbt_python.operators.dbt import (DbtRunOperator, DbtTestOperator)
from google.cloud import bigquery

# ─── ENV ───────────────────────────────────────────────────────────
DBT_PROJECT_DIR = os.getenv("DBT_PROJECT_DIR")  
DBT_PROFILES_DIR = os.getenv("DBT_PROFILES_DIR")
BQ_DATASET = os.getenv("BQ_DBT_DATASET")
PROJECT_ID = os.getenv("GCP_PROJECT_ID")

# ─── CONST ─────────────────────────────────────────────────────────
default_args = {
    'owner': 'lana',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# ─── HELPER FUNCTION ───────────────────────────────────────────────
def check_new_data(**context):
    """
    Check if new data exists in forecast_raw or current_raw tables
    since the last 4-hourly ingestion run (based on rounded logical_date).
    Returns True if new data exists in at least one table.
    """
    client = bigquery.Client(project=PROJECT_ID)
    ti = context['ti']
    logical_date = context["logical_date"]

    # Round down to nearest 4-hour block
    current_hour = (logical_date.hour // 4) * 4 
    rounded_now = logical_date.replace(hour=current_hour, minute=0, second=0, microsecond=0) 
    last_run = rounded_now - timedelta(hours=4)

    print(f"Checking for new data after last run at: {last_run.isoformat()}")

    tables = ['forecast_raw', 'current_raw']
    new_data_exists = False

    for table in tables:
        try:
            query = f"""
                SELECT COUNT(*) AS cnt
                FROM `{PROJECT_ID}.raw.{table}`
                WHERE fetched_at > TIMESTAMP('{last_run.isoformat()}')
            """
            result = client.query(query).result()
            count = next(result).cnt
            if count > 0:
                print(f"Found {count} new records in {table} since last run")
                new_data_exists = True
            else:
                print(f"No new data in {table} since last run")
        except Exception as e:
            print(f"Error checking {table}: {str(e)}")

    ti.xcom_push(key='new_data', value=new_data_exists)

# ─── DAG ───────────────────────────────────────────────────────────
with DAG(
    dag_id='dbt_airflow_pipeline',
    default_args=default_args,
    description='Run dbt after ingestion, only if new data exists',
    schedule='7 */4 * * *',
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=['dbt', 'bigquery'],
) as dag:

    check_new_data_task = PythonOperator(
        task_id='check_new_data',
        python_callable=check_new_data,
    )

    def branch_dbt(**context):
        ti = context['ti']
        if ti.xcom_pull(key='new_data', task_ids='check_new_data'):
            return 'dbt_run'
        else:
            return 'skip_dbt'

    branch_task = BranchPythonOperator(
        task_id='branch_dbt',
        python_callable=branch_dbt,
    )

    dbt_run = DbtRunOperator(
        task_id='dbt_run',
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile='dbt_air',
        trigger_rule='none_failed_min_one_success' 
    )

    dbt_test = DbtTestOperator(
        task_id='dbt_test',
        project_dir=DBT_PROJECT_DIR,
        profiles_dir=DBT_PROFILES_DIR,
        profile='dbt_air',
        trigger_rule='all_success'
    )

    skip_dbt = EmptyOperator(task_id='skip_dbt')  

    # ─── DAG DEPENDENCIES ─────────────────────────────────────────
    check_new_data_task >> branch_task
    branch_task >> dbt_run >> dbt_test
    branch_task >> skip_dbt