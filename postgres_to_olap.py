#this is a dag
import psycopg2
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
conn = BaseHook.get_connection('my_postgres')
psycopg2_conn = psycopg2.connect(database=conn.schema, user=conn.login, password=conn.password, host=conn.host, port=conn.port)
cursor = psycopg2_conn.cursor()

def test_connection():
    cursor.execute("SELECT 1")
    result = cursor.fetchone()
    logging.info(f"Connection test result: {result}")

def extract_data(**kwargs):
    cursor.execute("SELECT * FROM \"Receipts\"")
    data = cursor.fetchall()
    logging.info(f"Extracted data: {data}")  # Log the extracted data
    kwargs['ti'].xcom_push(key='extracted_data', value=data)

def transform_data(**kwargs):
    data = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_data')
    df = pd.DataFrame(data)
    # Transform the data into an OLAP cube CSV here
    csv_path = '/home/musab/Documents/Data Warehousing Project/olap_cube.csv'  # Specify the path to save the CSV
    df.to_csv(csv_path)
    logging.info(f"Transformed data saved to {csv_path}")  # Log the path of the saved CSV

with DAG('postgres_to_olap', start_date=datetime(2022, 1, 1), schedule_interval=None, catchup=False) as dag:
    test_connection_task = PythonOperator(task_id='test_connection', python_callable=test_connection)
    extract_task = PythonOperator(task_id='extract_data', python_callable=extract_data, provide_context=True)
    transform_task = PythonOperator(task_id='transform_data', python_callable=transform_data, provide_context=True)

    # Set task dependencies
    test_connection_task >> extract_task >> transform_task

