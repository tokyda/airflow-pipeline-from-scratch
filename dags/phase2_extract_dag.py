from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import os
import shutil
import csv
import psycopg2

RAW_DIR = "/opt/airflow/data/raw"
SOURCE_DIR = "/opt/airflow/data/source"

IMDB_SOURCE = f"{SOURCE_DIR}/IMDB_Dataset.csv"
IMDB_RAW = f"{RAW_DIR}/imdb_reviews.csv"
RETAIL_RAW = f"{RAW_DIR}/online_retail_extract.csv"


def stage_imdb_reviews():
    """
    Copy IMDb reviews from source zone into raw staging zone.
    This simulates an external CSV file drop.
    """
    os.makedirs(RAW_DIR, exist_ok=True)
    shutil.copyfile(IMDB_SOURCE, IMDB_RAW)


def extract_online_retail_from_postgres():
    """
    Query OnlineRetail data from Postgres and write result to raw staging.
    """
    os.makedirs(RAW_DIR, exist_ok=True)

    conn = psycopg2.connect(
        host="postgres",
        dbname="airflow",
        user="airflow",
        password="airflow",
        port=5432,
    )

    cur = conn.cursor()
    cur.execute("SELECT * FROM retail.online_retail;")

    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]

    with open(RETAIL_RAW, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(colnames)
        writer.writerows(rows)

    cur.close()
    conn.close()


with DAG(
    dag_id="phase2_extract_and_stage",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["phase2"],
) as dag:

    imdb_to_raw = PythonOperator(
        task_id="stage_imdb_csv_to_raw",
        python_callable=stage_imdb_reviews,
    )

    retail_to_raw = PythonOperator(
        task_id="extract_online_retail_from_postgres_to_raw",
        python_callable=extract_online_retail_from_postgres,
    )

    # These can run independently
    [imdb_to_raw, retail_to_raw]
