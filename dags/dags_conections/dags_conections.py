from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os

sys.path.append(os.path.abspath("/opt/airflow/dags/"))

from etls.dag_api_extract import extract_api, transform_api
from etls.grammy_etl import extract_db, transform_db
from etls.spotify_etl import extract_csv, transform_csv
from etls.merge_load_data import merge_data, save_to_postgres, upload_to_drive

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 4),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'etl_dag',
    default_args=default_args,
    description='DAG para extraer, transformar y cargar datos en PostgreSQL y Google Drive',
    schedule_interval=timedelta(days=1), 
    catchup=False, 
) as dag:

    lastfm_extract_task = PythonOperator(
        task_id='extract_lastfm',
        python_callable=extract_api,
    )

    grammy_extract_task = PythonOperator(
        task_id='extract_grammy',
        python_callable=extract_db
    )

    spotify_extract_task = PythonOperator(
        task_id='extract_spotify',
        python_callable=extract_csv
    )

    lastfm_transform_task = PythonOperator(
        task_id='transform_lastfm',
        python_callable=transform_api,
        op_kwargs={'df_lastfm': '{{ ti.xcom_pull(task_ids="extract_lastfm") }}'}
    )

    grammy_transform_task = PythonOperator(
        task_id='transform_grammy',
        python_callable=transform_db,
        op_kwargs={'df_grammy': '{{ ti.xcom_pull(task_ids="extract_grammy") }}'}
    )

    spotify_transform_task = PythonOperator(
        task_id='transform_spotify',
        python_callable=transform_csv,
        op_kwargs={'df_spotify': '{{ ti.xcom_pull(task_ids="extract_spotify") }}'}
    )

    merge_task = PythonOperator(
        task_id='merge_data',
        python_callable=merge_data,
        op_kwargs={
            'df_lastfm': '{{ ti.xcom_pull(task_ids="transform_lastfm") }}',
            'df_grammy': '{{ ti.xcom_pull(task_ids="transform_grammy") }}',
            'df_spotify': '{{ ti.xcom_pull(task_ids="transform_spotify") }}'
        }
    )

    load_db_task = PythonOperator(
        task_id='load_db_task',
        python_callable=save_to_postgres,
        op_kwargs={'df_merged': '{{ ti.xcom_pull(task_ids="merge_data") }}'}
    )

    upload_to_drive_task = PythonOperator(
        task_id='upload_to_drive',
        python_callable=upload_to_drive,
        op_kwargs={'df_merged': '{{ ti.xcom_pull(task_ids="merge_data") }}'}
    )

    lastfm_extract_task >> lastfm_transform_task
    grammy_extract_task >> grammy_transform_task
    spotify_extract_task >> spotify_transform_task
    [lastfm_transform_task, grammy_transform_task, spotify_transform_task] >> merge_task
    merge_task >> [load_db_task, upload_to_drive_task]
