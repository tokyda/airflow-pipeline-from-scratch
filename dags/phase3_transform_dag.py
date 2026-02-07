from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime


with DAG(
    dag_id="phase3_transform",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    clean_imdb_reviews = BashOperator(
        task_id="clean_imdb_reviews",
        bash_command="python /opt/airflow/dags/scripts/clean_imdb_reviews.py",
    )

    clean_online_retail = BashOperator(
        task_id="clean_online_retail",
        bash_command="python /opt/airflow/dags/scripts/clean_online_retail.py",
    )

    build_customer_metrics = BashOperator(
        task_id="build_customer_metrics",
        bash_command="python /opt/airflow/dags/scripts/build_customer_metrics.py",
    )

    generate_dashboard = BashOperator(
    task_id="generate_dashboard",
    bash_command="python /opt/airflow/dashboards/generate_dashboard.py",
    )


    clean_imdb_reviews >> clean_online_retail >> build_customer_metrics >> generate_dashboard
