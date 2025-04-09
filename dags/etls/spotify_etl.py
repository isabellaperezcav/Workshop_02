from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import ast  # Para convertir la lista de artistas en strings limpios

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

DATA_DIR = "/home/isabella/Escritorio/Worksho_002/dags/data"
os.makedirs(DATA_DIR, exist_ok=True)

def extract_csv():
    """Lee el CSV de Spotify de la ubicación original y lo guarda temporalmente."""
    try:
        df = pd.read_csv("/home/isabella/Escritorio/Worksho_002/Spotify/spotify_dataset.csv")

        # Guardar el archivo extraído
        df.to_csv(f"{DATA_DIR}/spotify_extracted.csv", index=False)
        print("✅ CSV extraído y guardado en spotify_extracted.csv")

    except Exception as e:
        print(f"❌ Error en la extracción del CSV de Spotify: {e}")

def transform_csv():
    """Realiza transformaciones al CSV de Spotify para el merge."""
    try:
        df = pd.read_csv(f"{DATA_DIR}/spotify_extracted.csv")

        # Normalizar track_name
        df["track_name"] = df["track_name"].astype(str).str.strip().str.lower()

        # Normalizar artistas (convertir listas de strings a string simple)
        def clean_artist(artist_str):
            try:
                artists_list = ast.literal_eval(artist_str)  # Convierte el string a lista
                if isinstance(artists_list, list):
                    return artists_list[0].strip().lower()  # Solo tomamos el primer artista
                return artist_str.strip().lower()
            except (ValueError, SyntaxError):
                return artist_str.strip().lower()

        df["artists"] = df["artists"].astype(str).apply(clean_artist)

        # Extraer solo el año de la fecha de lanzamiento
        df["release_year"] = pd.to_datetime(df["release_date"], errors="coerce").dt.year

        # Seleccionar columnas necesarias
        df = df[["track_name", "artists", "popularity", "release_year"]]

        # Eliminar duplicados
        df.drop_duplicates(subset=["track_name", "artists"], inplace=True)

        # Guardar el dataset transformado
        df.to_csv(f"{DATA_DIR}/spotify_transformed.csv", index=False)
        print("✅ CSV transformado y guardado en spotify_transformed.csv")

    except Exception as e:
        print(f"❌ Error en la transformación del CSV de Spotify: {e}")

with DAG(
    "dag_csv_spotify",
    default_args=default_args,
    description="DAG para extraer y transformar CSV de Spotify",
    schedule_interval="@daily",
) as dag_csv_spotify:

    t1_extract = PythonOperator(
        task_id="extract_csv",
        python_callable=extract_csv
    )

    t2_transform = PythonOperator(
        task_id="transform_csv",
        python_callable=transform_csv
    )

    t1_extract >> t2_transform
