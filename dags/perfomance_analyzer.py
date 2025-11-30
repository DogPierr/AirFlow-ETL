"""
Airflow DAG для анализа производительности сервисов (каждые 2 часа)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

from src.analysis.errors_analysis import run_performance_analysis, check_pipeline_stop_condition

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'performance_analysis',
    default_args=default_args,
    description='Анализ производительности сервисов каждые 2 часа',
    schedule_interval='0 */2 * * *',
    catchup=False,
    tags=['analysis', 'performance', 'bi_hourly'],
) as dag:
    check_stop_condition_task = PythonOperator(
        task_id='check_pipeline_stop_condition',
        python_callable=check_pipeline_stop_condition,
        provide_context=True,
        dag=dag,
    )

    check_stop_condition_task