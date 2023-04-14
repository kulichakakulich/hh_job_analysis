from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 4, 14),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'scrape_and_add_vacancies',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

scrape_vacancies = BashOperator(
    task_id='scrape_vacancies',
    bash_command='python /path/to/scrape_vacancies.py',
    dag=dag
)

connect_bd = BashOperator(
    task_id='connect_bd',
    bash_command='python /path/to/connect_bd.py',
    dag=dag
)
