import requests

# URL de la API de OpenStreetMap
url = "https://www.last.fm/api"

# Agregar User-Agent para evitar bloqueos
headers = {"User-Agent": "Mozilla/5.0"}

# Hacer la solicitud GET
response = requests.get(url, headers=headers)

# Verificar la respuesta
if response.status_code == 200:
    print("Conexión exitosa a la API")
else:
    print(f"Error en la solicitud: Código {response.status_code}")
