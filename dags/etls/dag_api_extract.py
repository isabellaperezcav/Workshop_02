from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 4, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

API_KEY = "2352e920e4b1746633b69ebcaa090e5e" 
URL = "http://ws.audioscrobbler.com/2.0/"
DATA_DIR = "/home/isabella/Escritorio/Worksho_002/dags/data"
os.makedirs(DATA_DIR, exist_ok=True)

TRACKS = ["Shape of You", "Blinding Lights", "Rolling in the Deep", "Someone Like You", "Bohemian Rhapsody"]

def extract_api():
    """Extrae datos de Last.fm por cada track en la lista y guarda un CSV."""
    all_data = []

    for track in TRACKS:
        params = {
            "method": "track.search",
            "track": track,
            "api_key": API_KEY,
            "format": "json",
            "limit": 5
        }
        try:
            response = requests.get(URL, params=params)
            response.raise_for_status()
            data = response.json()

            if "error" in data:
                raise ValueError(f"Error en la API: {data.get('message', 'Desconocido')}")

            tracks = data.get("results", {}).get("trackmatches", {}).get("track", [])

            if not tracks:
                print(f"⚠️ No se encontraron datos para {track}.")
                continue

            for t in tracks:
                all_data.append({
                    "track_name": t.get("name", "Unknown"),
                    "artist": t.get("artist", "Unknown"),
                    "listeners": t.get("listeners", 0),
                    "playcount": t.get("playcount", 0)
                })

        except requests.exceptions.RequestException as e:
            print(f"❌ Error en la solicitud a Last.fm: {e}")
        except ValueError as e:
            print(f"❌ Error en la respuesta de la API: {e}")

    if all_data:
        df = pd.DataFrame(all_data)
        df.to_csv(f"{DATA_DIR}/lastfm_extracted.csv", index=False)
        print("✅ Datos extraídos de Last.fm y guardados en lastfm_extracted.csv")
    else:
        print("⚠️ No se generaron datos en el CSV.")

def transform_api():
    """Transforma los datos extraídos de la API de Last.fm."""
    try:
        df = pd.read_csv(f"{DATA_DIR}/lastfm_extracted.csv")

        for col in ["listeners", "playcount"]:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        df.drop_duplicates(subset=["artist", "track_name"], inplace=True)

        df.to_csv(f"{DATA_DIR}/lastfm_transformed.csv", index=False)
        print("✅ Datos de Last.fm transformados y guardados en lastfm_transformed.csv")

    except Exception as e:
        print(f"❌ Error en la transformación de datos: {e}")

with DAG(
    "dag_api_lastfm",
    default_args=default_args,
    description="DAG para extraer y transformar datos de la API de Last.fm",
    schedule_interval="@daily",
) as dag_api_lastfm:

    t1_extract = PythonOperator(
        task_id="extract_api",
        python_callable=extract_api
    )

    t2_transform = PythonOperator(
        task_id="transform_api",
        python_callable=transform_api
    )

    t1_extract >> t2_transform
