from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

# A DAG is just a workflow definition
with DAG(
    dag_id="phase1_hello_dag",
    start_date=datetime(2025, 1, 1),
    schedule=None,          # manual runs only
    catchup=False,
    tags=["phase1"],
) as dag:

    extract = BashOperator(
        task_id="extract",
        bash_command="echo 'extract step: hello world'",
    )

    process = BashOperator(
        task_id="process",
        bash_command="echo 'process step: transforming...'",
    )

    dashboard = BashOperator(
        task_id="dashboard",
        bash_command="echo 'dashboard step: generating report...'",
    )

    # Dependencies (order): extract -> process -> dashboard
    extract >> process >> dashboard
