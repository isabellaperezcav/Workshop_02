import psycopg2
import os
from dotenv import load_dotenv

# Cargar variables del .env
load_dotenv()

def conectar_db():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            options="-c client_encoding=UTF8"
        )
        conn.set_client_encoding('UTF8')  # Asegurar codificación UTF-8
        print("✅ Conexión exitosa con psycopg2")
        return conn
    except Exception as e:
        print("❌ Error en conexión con psycopg2:", e)
        return None
