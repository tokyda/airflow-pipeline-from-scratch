from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="phase3_transform_imdb",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    clean_imdb_reviews = BashOperator(
        task_id="clean_imdb_reviews",
        bash_command="python /opt/airflow/dags/scripts/clean_imdb_reviews.py",
    )

