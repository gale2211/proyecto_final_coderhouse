from datetime import datetime, timedelta
from airflow import DAG
#from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
#from airflow.providers.postgres.operators.postgres import PostgresOperator

from scripts.main import main_func

default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=1)
}

with DAG(
    dag_id="dag_spotify",
    start_date=datetime(2023, 11, 28),
    catchup=False,
    schedule_interval="0 9 * * *",
    default_args=default_args
) as dag:

    # task con dummy operator
    dummy_start_task = EmptyOperator(
        task_id="start"
    )

    main_func_task = PythonOperator(
        task_id="main_func",
        python_callable=main_func,
        op_kwargs={
            "config_file": "/opt/airflow/config/config.ini"
        }
    )
    
    dummy_end_task = EmptyOperator(
        task_id="end"
    )

    dummy_start_task >> main_func_task
    main_func_task >> dummy_end_task
