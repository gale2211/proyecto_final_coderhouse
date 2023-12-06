# FUNCIONES UTILES

from configparser import ConfigParser
import sqlalchemy as sa


# 1) FUNCION PARA CHECKEAR QUE SE GENERO UNA TOKEN TEMPORAL
def chequeo_token_status(response):
    if response.status_code == 200:
        token = response.json()
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