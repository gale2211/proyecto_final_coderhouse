# Importo las librerias

import requests
import json
import pandas as pd
import os
from configparser import ConfigParser
from pathlib import Path
import sqlalchemy as sa

# FUNCIONES UTILES

# 1) FUNCION PARA CHECKEAR QUE SE GENERO UNA TOKEN TEMPORAL
def chequeo_token_status(response):
    if response.status_code == 200:
        token = response_token.json()
        status = response.status_code
    else:
        status = response.status_code
    return token, status

# 2) FUNCION PARA GENERAL EL URL CON ENDPOINT
def build_url(base_url, endpoint):
    endpoint_url = f"{base_url}/{endpoint}"
    return endpoint_url

# 3) FUNCION PARA OBTENER ARTISTAS DE LA PLAYLIST SIN DUPLICAR
def artistas_in_playlist(response_playlist):
    artistas = []

    for item in response_playlist["tracks"]["items"]:
        for artist in item["track"]["artists"]:
            artistas.append(artist["id"])

    artistas_sin_duplicados = list(set(artistas))
    return artistas_sin_duplicados

# 4) FUNCION PARA NO SUPERAR EL LIMITE DE GET (50 ARTISTAS)
def ajusto_largo_request(artistas_sin_duplicados):
    req_artistas = []
    if len(artistas_sin_duplicados) > 50:
        for x in range(min(50, 50)):
            req_artistas.append(artistas_sin_duplicados[x])
    else:
        req_artistas = artistas_sin_duplicados
    return req_artistas

# 5) Construye la cadena de conexión a la base de datos a partir de un archivo de configuración.
def build_conn_string(config_path, config_section):

    # Lee el archivo de configuración
    parser = ConfigParser()
    parser.read(config_path)

    # Lee la sección de configuración de Redshift
    config = parser[config_section]
    host = config["host"]
    port = config["port"]
    dbname = config["dbname"]
    username = config["username"]
    pwd = config["pwd"]

    # Construye la cadena de conexión
    conn_string = (
        f"postgresql://{username}:{pwd}@{host}:{port}/{dbname}?sslmode=require"
    )
    return conn_string

# 6) Crea una conexión a la base de datos.
def connect_to_db(conn_string):
    """
    Crea una conexión a la base de datos.
    """
    engine = sa.create_engine(conn_string)
    conn = engine.connect()
    return conn, engine


# Cargar las credenciales en una variable
config = ConfigParser()
config_dir = "config.ini"
config.read(config_dir)

# Conseguimos la key temporal
data = {
    "grant_type": "client_credentials",
    "client_id": config["spotify"]["client_id"],
    "client_secret": config["spotify"]["client_secret"],
}
response_token = requests.post("https://accounts.spotify.com/api/token", data=data)
token, status = chequeo_token_status(response_token)

# Obtengo una lista automatica de IDs para los artistas de la playlist top 50 de argentina.
# Gracias a la funcion artistas_in_playlist no tengo id duplicados.
# Debido a que tambien hay canciones que pueden estar cantadas por mas de un artista, la funcion ajusto_largo_request hace que no me exceda de limite del request
base_url = "https://api.spotify.com/v1"
endpoint_playlist = "playlists"
endpoint_url = build_url(base_url, endpoint_playlist)
headers = {
    "Client-ID": data["client_id"],
    "Authorization": f"Bearer {token['access_token']}",
    "Acept": "application/json",
}
params_playlist = {
    "fields": "tracks(items(track(artists(id))))",
}
response_playlist = requests.get(
    "https://api.spotify.com/v1/playlists/37i9dQZEVXbMDoHDwVN2tF",
    params=params_playlist,
    headers=headers,
)
artistas_sin_duplicados = artistas_in_playlist(response_playlist.json())
req_artistas = ajusto_largo_request(artistas_sin_duplicados)

# Obtencion de datos de los artistas ya ajustados al maximo de 50 y sin duplicar
base_url = "https://api.spotify.com/v1"
endpoint_artists = "artists"
endpoint_url = build_url(base_url, endpoint_artists)

params_artist = {
    "ids": ",".join(req_artistas)
}
response_data = requests.get(endpoint_url, params=params_artist, headers=headers)

# Separo los artistas para que me queden uno por fila con sus metadatos
new = []
for artists in response_data.json()["artists"]:
    new.append(artists)

# Limpio un poco el dataframe
df = pd.DataFrame(new)
nuevos_nombres_columnas = {
    "external_urls": "links",
    "id": "artist_id",
    "name": "artist_name",
}
df = df.rename(columns=nuevos_nombres_columnas)
df["followers"] = df["followers"].apply(pd.Series)["total"]
new_df = df[["artist_id", "artist_name", "genres", "followers", "popularity", "links"]]
new_df["genres"] = new_df["genres"].apply(json.dumps)
new_df["links"] = new_df["links"].apply(json.dumps)

# DATA A REDSHIFT
# CONEXION A LA BASE DE DATOS DE REDSHIFT
conn_str = build_conn_string(config_dir, "redshift")
conn, engine = connect_to_db(conn_str)
schema = "guilleale22_coderhouse"
conn.execute(
    f"""
        DROP TABLE IF EXISTS {schema}.artistas_top_50_global;
        CREATE TABLE {schema}.artistas_top_50_global (
            artists_id VARCHAR(250),
            artist_name VARCHAR(250),
            genres TEXT,
            followers FLOAT,
            popularity INT,
            links TEXT
        );
    """
)


new_df.to_sql(
    name="artistas_top_50_global",
    con=conn,
    schema=schema,
    if_exists="replace",
    method="multi",
    index=False,
)
