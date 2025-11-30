from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

from src.analysis.descriptive_analysis import run_descriptive_analysis

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
    'descriptive_analysis',
    default_args=default_args,
    description='Описательный анализ данных каждый час',
    schedule_interval='0 * * * *',
    catchup=False,
    tags=['analysis', 'descriptive', 'hourly'],
) as dag:

    descriptive_analysis_task = PythonOperator(
        task_id='run_descriptive_analysis',
        python_callable=run_descriptive_analysis,
        provide_context=True,
        dag=dag,
    )