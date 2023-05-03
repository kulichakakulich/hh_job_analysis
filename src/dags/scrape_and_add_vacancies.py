from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from connect_bd import connect_database

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'scrape_and_add_vacancies',
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        catchup=False
) as dag:
    scrape_vacancies = BashOperator(
        task_id='scrape_vacancies',
        bash_command='python /opt/airflow/dags/scrape_vacancies.py',
        dag=dag
    )
    connect_bd = PythonOperator(
        task_id='connect_bd',
        python_callable=connect_database,
        dag=dag
    )
    scrape_vacancies >> connect_bd

