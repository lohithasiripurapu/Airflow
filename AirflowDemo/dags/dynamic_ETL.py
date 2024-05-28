from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
import os
import pandas as pd
import requests

CONFIG_API_URL = 'http://flask-api:5000/config'

def fetch_config_from_api():
    response = requests.get(CONFIG_API_URL)
    response.raise_for_status()  # Raise an exception for HTTP errors
    return response.json()

# def fetch_config(**kwargs):
#     return kwargs['dag_run'].conf


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 5, 12),
    'retries': 1,
    'retry_delay': timedelta(minutes = 5)
}

def ingest_data(sql_query, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    
    cursor.execute(sql_query)
    rows = cursor.fetchall()
    column_names = [desc[0] for desc in cursor.description]
    df = pd.DataFrame(rows, columns=column_names)
    print(df)
    
    kwargs['ti'].xcom_push(key='ingestion_data', value=df.to_dict(orient='records'))
    
    cursor.close()
    connection.close()

def calculate_metrics(sql_query, **kwargs):
    postgres_hook = PostgresHook(postgres_conn_id='postgres_localhost')
    connection = postgres_hook.get_conn()
    cursor = connection.cursor()
    
    cursor.execute(sql_query)
    ans = cursor.fetchall()

    print(ans)
    
    cursor.close()
    connection.close()
    

with DAG(
    dag_id="ingest_tables1",
    default_args=default_args,
    schedule_interval=None,
) as dag:
    start_task = DummyOperator(task_id='start_task')
    end_task = DummyOperator(task_id='end_task')
    
    config = fetch_config_from_api()

    # Create dynamic tasks based on the configuration
    dynamic_tasks = {}
    for table_key, table_config in config['config']['ingest'].items():
        task_id = f"ingest_{table_config['table_name']}"
        sql_query = table_config['query']
        
        dynamic_task = PythonOperator(
            task_id=task_id,
            python_callable=ingest_data,
            op_kwargs={'sql_query': sql_query},
            provide_context=True,
            dag=dag
        )
        
        dynamic_tasks[task_id] = dynamic_task
        start_task >> dynamic_task
    
    
    calculate_metrics_tasks = {}
    for table_key, table_config in config["config"]["metrics"].items():
        
        task_id = f"calculate_metric_{table_config['metric_name']}"
        sql_query = table_config['query']
        

        calculate_metrics_task = PythonOperator(
            task_id=task_id,
            python_callable=calculate_metrics,
            op_kwargs={'sql_query': sql_query},
            provide_context=True,
            dag=dag
        )
        calculate_metrics_tasks[task_id] = calculate_metrics_task
        calculate_metrics_task >> end_task
    
    for task in dynamic_tasks.values():
        for cal_task in calculate_metrics_tasks.values():
            task >> cal_task

