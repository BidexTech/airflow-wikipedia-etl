from airflow.sdk import DAG
from pendulum import datetime
from datetime import timedelta
from airflow.providers.standard.operators.python import PythonOperator
import requests, gzip, shutil, os, psycopg2
import pandas as pd
from sqlalchemy import create_engine
from sentiment.main import engine  
from dotenv import load_dotenv

# storage directory for data files mounted on docker volume

#download data and save to /tmp
URL = "https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251005-160000.gz"
FILE_NAME = "/opt/airflow/dags/sentiment/data/pageviews.gz"
OUTPUT_FILE = "/opt/airflow/dags/sentiment/data/pageviews.txt"
TARGET_FILE = "/opt/airflow/dags/sentiment/data/filtered_views.csv"

# ETL starts here
def _download_file():
    if os.path.exists(FILE_NAME):
        print(f"File already exists at {FILE_NAME}. Skipping download...")
        return FILE_NAME

    print("Downloading file from https://dumps.wikimedia.org/other/pageviews/2025/2025-10/pageviews-20251005-160000.gz ....")
    response = requests.get(URL, stream=True)
    if response.status_code == 200:
        with open(FILE_NAME, "wb") as file:
            file.write(response.content)
        print(f"File pageviews-20251005-160000.gz downloaded to: {FILE_NAME}")
    else:
        raise Exception(f" Download failed with status code {response.status_code}")
    

def _extract_file():
    print("Extract gzip file to plain text")
    with gzip.open(FILE_NAME, "rb") as file_in:
        with open(OUTPUT_FILE, "wb") as file_out:
            shutil.copyfileobj(file_in, file_out)
    print(f"Extracted to: {OUTPUT_FILE}")
    

def _transform_data():
    print(" Processing the extract data...")
    data = []
    with open(OUTPUT_FILE, "r", encoding="utf-8") as file:
        for line in file:
            parts = line.strip().split(" ")
            if len(parts) < 4:
                continue
            domain_code, page_title, views, size = parts
           #Accept all domain codes and filter only for specific page titles
            if page_title in ["Amazon", "Apple", "Facebook", "Google", "Microsoft"]:
                data.append({
                    "domain_code": domain_code,
                    "page_title": page_title,
                    "views": int(views)
                })      
              
    df = pd.DataFrame(data)
    df.to_csv(TARGET_FILE, index=False)
    print(f" Process data saved to: {TARGET_FILE}")
    

def _load_to_supabase():
    print(" Loading data into Supabase...")
    df = pd.read_csv(TARGET_FILE)
    df.to_sql("core_sentiment_pageviews", engine, if_exists="replace", index=False)
    print(f" Loaded {len(df)} records into Supabase!")


    print("ETL Pipeline completed successfully!.")

with DAG(
    dag_id="wikipedia_pageview",
    start_date=datetime(2025, 10, 22),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=3),
        "email_on_failure": True,
        "email": ["olasunkanmiabidemia@gmail.com"],
    },
):

    download_file = PythonOperator(
        task_id="download_file",
        python_callable=_download_file
    )

    extract_file = PythonOperator(
        task_id="extract_file",
        python_callable=_extract_file
    )

    transform_data = PythonOperator(
        task_id="transform_data",
        python_callable=_transform_data
    )

    load_to_supabase = PythonOperator(
        task_id="load_to_supabase",
        python_callable=_load_to_supabase
    )

    download_file >> extract_file >> transform_data >> load_to_supabase
