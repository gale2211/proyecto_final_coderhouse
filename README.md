# Introducción

Para este proyecto final, se utilizó la información proveniente de la API de Spotify. La misma posee una base de datos crudos con toda la información sobre los artistas, canciones, audiolibros y podcasts. El objetivo es poder obtener una base de datos con la información de los artistas que se encuentran en la lista de top 50 global. Para ello, se utilizó un script de python que extrajo la información de la API, transformó los datos mediante la librería pandas y por último los cargó en una tabla de redshift. Ese código se puede encontrar en el archivo Entregable_1.py y las credenciales en el archivo config.ini. 

# Files + Links

Los **archivos utilizados** para esta entrega son:

 1. Entregable_1.py: Contiene el código Python utilizado para la extracción y transformación de datos desde la API de Spotify.
 2. config.ini: Archivo de configuración que almacena las credenciales necesarias para acceder a la base de datos en Amazon Redshift y Spotify.

Link de la API de spotify con su documentación:

https://developer.spotify.com/documentation/web-api

# Proceso

El proceso de obtención y manipulación de datos se llevó a cabo en varias etapas:

 1. **Extracción de Datos:** Utilizando el script Entregable_1.py, se extrajo la información necesaria de la API de Spotify.
 2. **Transformación de Datos:** Se empleó la librería Pandas en Python para procesar y transformar los datos extraídos, preparándolos para su carga en la base de datos.
 3. **Carga en Redshift:** La información procesada se cargó en una tabla de Amazon Redshift, utilizando las credenciales que se encuentran en el archivo config.ini.

# Liberías de python instladas
 1. requests
 2. json
 3. pandas
 4. os
 5. configparser
 6. pathlib
 7. sqlalchemy

# Tablas Generadas en Redshift
Se utilizó la base de datos entregada por la cátedra de CoderHouse llamada "data-engineer-database". A su vez, el esquema donde se encuentra la tabla de los artistas del top 50 globlal se llama guilleale22_coderhouse.

Un ejemplo de SQL para obtener la información de la tabla sería:

select * from "data-engineer-database".guilleale22_coderhouse.artistas_top_50_global

DISTKEY(followers)
SORTKEY(popularity);

## Codigo DDL de la tabla


