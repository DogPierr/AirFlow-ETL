from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta

from src.extractor.weather import fetch_weather
from src.extractor.news import fetch_news
from src.extractor.crypto import fetch_crypto
from src.db import init_db

default_args = {
    "owner": "kita",
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}


with DAG(
    dag_id="collector",
    description="Pipeline: collect data from 3 sources every 3 minutes",
    default_args=default_args,
    schedule_interval="*/3 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["collector", "data", "pipeline"]
) as dag:

    t_init = PythonOperator(
        task_id="init_iteration",
        python_callable=init_db,
        provide_context=True
    )

    with TaskGroup("fetch_sources") as fetch_group:
        t_weather = PythonOperator(
            task_id="fetch_weather",
            python_callable=fetch_weather,
            provide_context=True
        )
    
        t_news = PythonOperator(
            task_id="fetch_news",
            python_callable=fetch_news,
            provide_context=True
        )
        
        t_crypto = PythonOperator(
            task_id="fetch_crypto",
            python_callable=fetch_crypto,
            provide_context=True
        )
    

    t_init >> fetch_group
