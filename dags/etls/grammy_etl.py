from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATA_DIR = "/home/isabella/Escritorio/Worksho_002/dags/data"
os.makedirs(DATA_DIR, exist_ok=True)

def extract_db():
    """Extrae datos de la base de datos de Grammy y los guarda como CSV."""
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT")
        )
        query = """
        SELECT year, category, nominee, artist, winner 
        FROM grammys_data;
        """
        df = pd.read_sql(query, conn)
        conn.close()

        # Guardar el archivo extraído
        df.to_csv(f"{DATA_DIR}/grammy_extracted.csv", index=False)
        print("✅ Datos de Grammy extraídos y guardados en grammy_extracted.csv")

    except Exception as e:
        print("❌ Error en la extracción de la DB de Grammy:", e)

def transform_db():
    """Transforma los datos de los Grammy para el merge."""
    try:
        df = pd.read_csv(f"{DATA_DIR}/grammy_extracted.csv")

        # Normalizar nombres de artista y canción
        df["artist"] = df["artist"].astype(str).str.strip().str.lower()
        df["nominee"] = df["nominee"].astype(str).str.strip().str.lower()

        # Renombrar columnas para mantener consistencia
        df.rename(columns={"nominee": "track_name"}, inplace=True)

        # Eliminar duplicados por canción y artista
        df.drop_duplicates(subset=["track_name", "artist"], inplace=True)

        # Guardar el archivo transformado
        df.to_csv(f"{DATA_DIR}/grammy_transformed.csv", index=False)
        print("✅ Datos de Grammy transformados y guardados en grammy_transformed.csv")

    except Exception as e:
        print(f"❌ Error en la transformación de datos: {e}")

with DAG(
    "dag_db_grammy",
    default_args=default_args,
    description="DAG para extraer y transformar datos de la DB de Grammy",
    schedule_interval="@daily",
) as dag_db_grammy:

    t1_extract = PythonOperator(
        task_id="extract_db",
        python_callable=extract_db
    )

    t2_transform = PythonOperator(
        task_id="transform_db",
        python_callable=transform_db
    )

    t1_extract >> t2_transform
