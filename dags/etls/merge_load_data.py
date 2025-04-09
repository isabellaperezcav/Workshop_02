import pandas as pd
import json
import logging
import io
import os
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values 
from io import StringIO


load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = "/home/isabella/Escritorio/Worksho_002/dags/data"
LASTFM_TRANSFORMED_CSV_PATH = os.path.join(DATA_DIR, "lastfm_transformed.csv") 


SPOTIFY_CSV_PATH = os.getenv("SPOTIFY_CSV_PATH", "/home/isabella/Escritorio/Worksho_002/dags/data/spotify_extracted.csv") 


DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "host.docker.internal") 
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")

DRIVE_CREDENTIALS_PATH = os.getenv("DRIVE_CREDENTIALS_PATH")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1), 
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def merge_data(**kwargs):
    logger.info("Starting Merge Process")
    ti = kwargs["ti"] 

    try:
        if not os.path.exists(LASTFM_TRANSFORMED_CSV_PATH):
             logger.error(f"Last.fm transformed file not found at: {LASTFM_TRANSFORMED_CSV_PATH}")
             raise FileNotFoundError(f"Required input file not found: {LASTFM_TRANSFORMED_CSV_PATH}")
        df_lastfm = pd.read_csv(LASTFM_TRANSFORMED_CSV_PATH)
        logger.info(f"Data from Last.fm loaded successfully from {LASTFM_TRANSFORMED_CSV_PATH}.")
        df_lastfm['artist'] = df_lastfm['artist'].astype(str)
        df_lastfm['track_name'] = df_lastfm['track_name'].astype(str)

    except FileNotFoundError as e:
         logger.error(f"Failed to load Last.fm data: {e}")
         raise

    except Exception as e:
        logger.error(f"Error reading or processing Last.fm CSV {LASTFM_TRANSFORMED_CSV_PATH}: {e}")
        raise


    try:
        df_spotify = pd.read_csv(SPOTIFY_CSV_PATH)
        logger.info(f"Data from Spotify CSV loaded from {SPOTIFY_CSV_PATH}.")
        df_spotify['artist'] = df_spotify['artists'].astype(str)
        df_spotify['track_name'] = df_spotify['track_name'].astype(str)
    except Exception as e:
        logger.error(f"Error loading Spotify CSV: {e}")
        raise


    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        df_grammy = pd.read_sql("SELECT * FROM grammy_awards", con=conn)
        conn.close()
        logger.info("Data from Grammy Awards table loaded successfully.")
        df_grammy['artist'] = df_grammy['artist'].astype(str)

    except Exception as e:
        logger.error(f"Error loading Grammy data from PostgreSQL: {e}")
        raise



    try:
        df_merged = pd.merge(df_spotify, df_lastfm, on=["artist", "track_name"], how="outer")
        df_merged = pd.merge(df_merged, df_grammy, on=["artist"], how="outer")
        logger.info("All DataFrames merged successfully using 'outer' join.")

    except KeyError as e:
        logger.error(f"Error during merge - likely missing merge key column: {e}")
        logger.error(f"Columns in df_spotify: {df_spotify.columns.tolist()}")
        logger.error(f"Columns in df_lastfm: {df_lastfm.columns.tolist()}")
        logger.error(f"Columns in df_grammy: {df_grammy.columns.tolist()}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error during merge: {e}")
        raise

    # Rellenar valores nulos
    fill_values = {
        'listeners': 0,
        'playcount': 0,
        'popularity': 0,          
        'genre': 'Unknown',
        'album_name': 'Unknown',
        'year': 0,                
        'category': 'Unknown',
        'nominee': 'Unknown',     
        'workers': 'Unknown',
        'img': 'No Image',
        'winner': False           
    }

    existing_cols_fill = {k: v for k, v in fill_values.items() if k in df_merged.columns}
    df_merged.fillna(existing_cols_fill, inplace=True)
    logger.info("Null values filled.")

    try:
        if 'listeners' in df_merged.columns: df_merged['listeners'] = df_merged['listeners'].astype(int)
        if 'playcount' in df_merged.columns: df_merged['playcount'] = df_merged['playcount'].astype(int)
        if 'popularity' in df_merged.columns: df_merged['popularity'] = df_merged['popularity'].astype(int)
        if 'winner' in df_merged.columns: df_merged['winner'] = df_merged['winner'].astype(bool)
    except Exception as e:
        logger.warning(f"Could not convert column types after fillna: {e}")


    logger.info(f"Merge completed. Resulting DataFrame shape: {df_merged.shape}")
    # Devuelve el DataFrame como JSON para las tareas siguientes
    return df_merged.to_json(orient='records')

def save_to_postgres(**kwargs):
    logger.info("Saving data to PostgreSQL")
    ti = kwargs['ti']
    json_data = ti.xcom_pull(task_ids='merge_data')

    if json_data is None:
         logger.error("No JSON data received from merge_data task. Cannot save to PostgreSQL.")
         raise ValueError("Failed to retrieve merged data from XCom.")

    try:
        df = pd.read_json(io.StringIO(json_data), orient='records', convert_dates=False)
        logger.info(f"Successfully converted JSON from XCom to DataFrame with shape: {df.shape}")
    except Exception as e:
        logger.error(f"Failed to convert JSON from XCom to DataFrame: {e}")
        logger.error(f"Received data (first 500 chars): {str(json_data)[:500]}")
        raise

    if df.empty:
        logger.warning("Received empty DataFrame from merge_data task. Nothing to save to PostgreSQL.")
        return 


    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()

        table_name = 'merged_data'
        cursor.execute(f"DROP TABLE IF EXISTS {table_name}")

        columns_with_types = ', '.join([
            f'"{col}" TEXT' for col in df.columns  
        ])
        create_query = f'CREATE TABLE {table_name} ({columns_with_types});'
        cursor.execute(create_query)

        columns = ', '.join([f'"{col}"' for col in df.columns])
        values = [tuple(map(str, row)) for row in df.to_numpy()] 
        insert_query = f'INSERT INTO {table_name} ({columns}) VALUES %s'
        execute_values(cursor, insert_query, values)

        conn.commit()
        logger.info(f"Data saved to PostgreSQL successfully in table '{table_name}'")

    except Exception as e:
        logger.error(f"Failed to save data to PostgreSQL: {e}")
        raise
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()

def create_drive_service():
    SCOPES = ['https://www.googleapis.com/auth/drive']
    if not DRIVE_CREDENTIALS_PATH or not os.path.exists(DRIVE_CREDENTIALS_PATH):
        logger.error(f"Google Drive credentials path not found or not set: {DRIVE_CREDENTIALS_PATH}")
        raise FileNotFoundError("Google Drive credentials file not configured correctly.")
    try:
        credentials = service_account.Credentials.from_service_account_file(DRIVE_CREDENTIALS_PATH, scopes=SCOPES)
        service = build('drive', 'v3', credentials=credentials)
        logger.info("Google Drive service created successfully.")
        return service
    except Exception as e:
        logger.error(f"Failed to create Google Drive service: {e}")
        raise

def upload_to_drive(**kwargs):
    logger.info("Uploading data to Google Drive")
    ti = kwargs['ti']
    df_json = ti.xcom_pull(task_ids='merge_data')

    if df_json is None:
         logger.error("No JSON data received from merge_data task. Cannot upload to Google Drive.")
         raise ValueError("Failed to retrieve merged data from XCom.")

    try:
        df = pd.read_json(StringIO(df_json), orient='records', convert_dates=False)
        logger.info(f"Successfully converted JSON from XCom to DataFrame for Drive upload. Shape: {df.shape}")
    except Exception as e:
        logger.error(f"Failed to convert JSON from XCom to DataFrame for Drive upload: {e}")
        logger.error(f"Received data (first 500 chars): {str(df_json)[:500]}")
        raise

    if df.empty:
        logger.warning("Received empty DataFrame from merge_data task. Nothing to upload to Google Drive.")
        return 

    try:
        service = create_drive_service()
    except Exception as e:
        raise

    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_content = csv_buffer.getvalue()
    csv_buffer.close() 

    # Convertir a BytesIO para MediaIoBaseUpload
    byte_stream = io.BytesIO(csv_content.encode('utf-8')) 

    file_metadata = {
        'name': f'MergedData_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv', 
        'mimeType': 'text/csv',
        'parents': ['1ncY6d8R5kpfsZyqh0B-WFZdhlTe3mAGh']
    }

    media = MediaIoBaseUpload(byte_stream, mimetype='text/csv', resumable=True)

    try:
        logger.info(f"Uploading file '{file_metadata['name']}' to Google Drive...")
        file = service.files().create(
            body=file_metadata,
            media_body=media,
            fields='id, webViewLink' 
        ).execute()
        file_id = file.get('id')
        file_link = file.get('webViewLink')
        logger.info(f"File uploaded successfully to Google Drive. File ID: {file_id}, Link: {file_link}")

        try:
            permissions = {
                'type': 'anyone', 
                'role': 'reader', 
            }
            service.permissions().create(fileId=file_id, body=permissions).execute()
            logger.info(f"Permissions set successfully for file ID: {file_id} (anyone can read)")
        except Exception as e:
            logger.warning(f"Could not set permissions for file ID {file_id}: {e}") 

        return file_id 

    except Exception as e:
        logger.error(f"Failed to upload file to Google Drive: {e}")
        raise

# --- Definición del DAG ---
with DAG(
    dag_id="dag_merge_lastfm_spotify_grammy", 
    default_args=default_args,
    description="DAG para fusionar datos de Last.fm (CSV), Spotify (CSV) y Grammy (DB), guardar en PostgreSQL y Google Drive",
    schedule_interval="@daily",
    catchup=False 
) as dag:

    merge_task = PythonOperator(
        task_id="merge_data",
        python_callable=merge_data
    )

    save_db_task = PythonOperator(
        task_id="save_to_postgres",
        python_callable=save_to_postgres
    )

    upload_drive_task = PythonOperator(
        task_id="upload_to_drive",
        python_callable=upload_to_drive
    )

    merge_task >> [save_db_task, upload_drive_task]