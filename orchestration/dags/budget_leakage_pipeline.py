from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).parent.parent.parent))

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'budget_leakage_pipeline',
    default_args=default_args,
    description='Budget Leakage Detection Pipeline - Bronze to Gold',
    schedule_interval='@daily',
    start_date=days_ago(1),
    catchup=False,
    tags=['budget', 'leakage', 'etl'],
)

project_root = Path(__file__).parent.parent.parent

bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command=f'cd {project_root} && python pipelines/pyspark/bronze_to_silver.py',
    dag=dag,
)

silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command=f'cd {project_root} && python pipelines/pyspark/silver_to_gold.py',
    dag=dag,
)

kpis_and_flags = BashOperator(
    task_id='kpis_and_flags',
    bash_command=f'cd {project_root} && python pipelines/pyspark/kpis_and_flags.py',
    dag=dag,
)

bronze_to_silver >> silver_to_gold >> kpis_and_flags

