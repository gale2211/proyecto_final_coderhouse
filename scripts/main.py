# Importo las librerias
import requests
import logging
import json
import pandas as pd
from configparser import ConfigParser
import sqlalchemy as sa
from datetime import date
from functions import *

def obtengo_informacion_api():
    # Cargar las credenciales en una variable
    config = ConfigParser()
    config_dir = "config/config.ini"
    config.read(config_dir)

    # Conseguimos la key temporal
    data = {
        "grant_type": "client_credentials",
        "client_id": config["spotify"]["client_id"],
        "client_secret": config["spotify"]["client_secret"],
    }
    try:
        response_token = requests.post("https://accounts.spotify.com/api/token", data=data)
        token, status = chequeo_token_status(response_token)
        logging.info("Clave temporal conseguida exitosamente")
    except Exception as e:
        logging.error(f"Error en la obtención de la key temporal: {e}")    

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
    try:
        response_playlist = requests.get(
            "https://api.spotify.com/v1/playlists/37i9dQZEVXbMDoHDwVN2tF",
            params=params_playlist,
            headers=headers,
        )
        artistas_sin_duplicados = artistas_in_playlist(response_playlist.json())
        req_artistas = ajusto_largo_request(artistas_sin_duplicados)
        logging.info("Artistas conseguidos exitosamente")
    except Exception as e:
        logging.error(f"Error en la obtención de la informacion de los artistas: {e}")
    # Obtencion de datos de los artistas ya ajustados al maximo de 50 y sin duplicar
    base_url = "https://api.spotify.com/v1"
    endpoint_artists = "artists"
    endpoint_url = build_url(base_url, endpoint_artists)

    params_artist = {
        "ids": ",".join(req_artistas)
    }
    try:
        response_data = requests.get(endpoint_url, params=params_artist, headers=headers)
        logging.info("Artistas sin duplicados exitosamente")
    except Exception as e:
        logging.error(f"Error al conseguir los artistas sin duplicar: {e}")
    # Separo los artistas para que me queden uno por fila con sus metadatos
    new = []
    for artists in response_data.json()["artists"]:
        new.append(artists)
    return new

# Limpio un poco el dataframe
def limpio_data_frame (new):
    df = pd.DataFrame(new)
    nuevos_nombres_columnas = {
        "external_urls": "links",
        "id": "artists_id",
        "name": "artist_name",
    }
    df = df.rename(columns=nuevos_nombres_columnas)
    df["followers"] = df["followers"].apply(pd.Series)["total"]
    new_df = df[["artists_id", "artist_name", "genres", "followers", "popularity", "links"]]
    new_df["genres"] = new_df["genres"].apply(json.dumps)
    new_df["links"] = new_df["links"].apply(json.dumps)
    today = date.today()
    new_df["updated_at"] = today
    return new_df

# DATA A REDSHIFT
# CONEXION A LA BASE DE DATOS DE REDSHIFT
def carga_datos_redshift(new_df): 
    config_dir = "config/config.ini"
    conn_str = build_conn_string(config_dir, "redshift")
    conn, engine = connect_to_db(conn_str)
    schema = "guilleale22_coderhouse"
    today = date.today()

    try:
        fecha_max = pd.read_sql_query('select max(updated_at)::date as max_date from guilleale22_coderhouse.artistas_top_50_global', conn)
        fecha_max['max_date'] = pd.to_datetime(fecha_max['max_date']).dt.date
        if today == fecha_max["max_date"][0]:
            conn.execute(
            f"""
                delete from guilleale22_coderhouse.artistas_top_50_global where updated_at = current_date 
            """
            )

            new_df.to_sql(
                name="artistas_top_50_global",
                con=conn,
                schema=schema,
                if_exists="append",
                method="multi",
                index=False,
            )
        else:
            new_df.to_sql(
                name="artistas_top_50_global",
                con=conn,
                schema=schema,
                if_exists="append",
                method="multi",
                index=False,
            )
        logging.info("Datos cargados exitosamente")
    except Exception as e:
        logging.error(f"Error al cargar los datos en la base de datos: {e}")
def main_func ():
    df = obtengo_informacion_api()
    new_df = limpio_data_frame(df)
    carga_datos_redshift(new_df)