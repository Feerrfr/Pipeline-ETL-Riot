import requests
import pandas as pd
import pyarrow as pa
from configparser import ConfigParser
from deltalake import write_deltalake, DeltaTable
from deltalake.exceptions import TableNotFoundError
from datetime import datetime, timedelta
from pprint import pprint
import streamlit as st

def get_data(base_url, endpoint, data_field=None, params=None, headers=None):
    """
    Realiza una solicitud GET a una API para obtener datos.

    Parámetros:
    base_url (str): La URL base de la API.
    endpoint (str): El endpoint de la API al que se realizará la solicitud.
    data_field (str): Atribudo del json de respuesta donde estará la lista
    de objetos con los datos que requerimos
    params (dict): Parámetros de consulta para enviar con la solicitud.
    headers (dict): Encabezados para enviar con la solicitud.

    Retorna:
    dict: Los datos obtenidos de la API en formato JSON.
    """
    try:
        endpoint_url = f"{base_url}/{endpoint}"
        response = requests.get(endpoint_url, params=params, headers=headers)
        response.raise_for_status()  # Levanta una excepción si hay un error en la respuesta HTTP.

        # Verificar si los datos están en formato JSON.
        try:
            data = response.json()
            if data_field:
              data = data[data_field]
        except:
            print("El formato de respuesta no es el esperado")
            return None
        return data

    except requests.exceptions.RequestException as e:
        # Capturar cualquier error de solicitud, como errores HTTP.
        print(f"La petición ha fallado. Código de error : {e}")
        print(f"Solicitar API_KEY válida si el error persiste.")
        print(f"Saliendo del script...")
        exit()
        return None



def build_table(json_data):
    """
    Construye un DataFrame de pandas a partir de datos en formato JSON.

    Parámetros:
    json_data (dict): Los datos en formato JSON obtenidos de una API.

    Retorna:
    DataFrame: Un DataFrame de pandas que contiene los datos.
    """
    try:
        df = pd.json_normalize(json_data)
        return df
    except:
        print("Los datos no están en el formato esperado")
        return None
    
def save_data_as_delta(df, path, storage_options, mode="overwrite", partition_cols=None):
    """
    Guarda un dataframe en formato Delta Lake en la ruta especificada.
    A su vez, es capaz de particionar el dataframe por una o varias columnas.
    Por defecto, el modo de guardado es "overwrite".

    Args:
      df (pd.DataFrame): El dataframe a guardar.
      path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      mode (str): El modo de guardado. Son los modos que soporta la libreria
      deltalake: "overwrite", "append", "error", "ignore".
      partition_cols (list or str): La/s columna/s por las que se particionará el
      dataframe. Si no se especifica, no se particionará.
    """
    write_deltalake(
        path, df, mode=mode, storage_options=storage_options, partition_by=partition_cols, schema_mode="merge"
    )

def save_new_data_as_delta(new_data, data_path, predicate, storage_options, partition_cols=None):
    """
    Guarda solo nuevos datos en formato Delta Lake usando la operación MERGE,
    comparando los datos ya cargados con los datos que se desean almacenar
    asegurando que no se guarden registros duplicados.

    Args:
      new_data (pd.DataFrame): Los datos que se desean guardar.
      data_path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      predicate (str): La condición de predicado para la operación MERGE.
    """

    try:
      dt = DeltaTable(data_path, storage_options=storage_options)
      new_data_pa = pa.Table.from_pandas(new_data)
      # Se insertan en target, datos de source que no existen en target
      dt.merge(
          source=new_data_pa,
          source_alias="source",
          target_alias="target",
          predicate=predicate
      ) \
      .when_not_matched_insert_all() \
      .execute()

    # Si no existe la tabla Delta Lake, se guarda como nueva
    except TableNotFoundError:
      save_data_as_delta(new_data, data_path, storage_options=storage_options, partition_cols=partition_cols)

def upsert_data_as_delta(data, data_path, predicate, storage_options):
    """
    Guardar datos en formato Delta Lake usando la operacion MERGE.
    Cuando no haya registros coincidentes, se insertarán nuevos registros.
    Cuando haya registros coincidentes, se actualizarán los campos.

    Args:
      data (pd.DataFrame): Los datos que se desean guardar.
      data_path (str): La ruta donde se guardará el dataframe en formato Delta Lake.
      predicate (str): La condición de predicado para la operación MERGE.
    """
    try:
        dt = DeltaTable(data_path)
        data_pa = pa.Table.from_pandas(data)
        dt.merge(
            source=data_pa,
            source_alias="source",
            target_alias="target",
            predicate=predicate
        ) \
        .when_matched_update_all() \
        .when_not_matched_insert_all() \
        .execute()
    except TableNotFoundError:
        save_data_as_delta(data, data_path, storage_options)

#--------------------------------------------- CONFIGURACION DE PIPELINE

parser = ConfigParser()                                             # Sirve para entrar a pipeline.conf y sacar las credenciales de ahi
parser.optionxform = str
parser.read("pipeline.conf")
api_credentials= st.secrets["api-credentials"]

api_key= api_credentials["api_key"]   #Cambia cada 24hs
headers = {"X-Riot-Token": api_key}

storage_options = dict(st.secrets["mysql-db"])
bkt_name = storage_options["bkt_name"]

#----------------------------------------------
def extraccion_lolstats():
    region="americas"
    gameName="Sebax"
    tagLine="100"

    url_base= f"https://{ region}.api.riotgames.com"
    endpoint= f"riot/account/v1/accounts/by-riot-id/{gameName}/{tagLine}"                   #este primer llamado a la api recoje valores estaticos como lo son el puuid, gameTag y tagLine

    #--------------------------------------------------- Primer llamado a la api para ver que los datos del jugador a analizar
    usuario_json = get_data(url_base, endpoint,headers=headers)
    puuidUsuario = usuario_json['puuid']

    #--------------------------------------------------- Guardado full del perfil del usuario en bronze 
    perfil_df = pd.DataFrame([usuario_json])
    ruta_perfil = "s3://fernandofranco-bucket/datalake/bronze/riotgames_api/summoner_profile/"
    write_deltalake(ruta_perfil, perfil_df, mode="overwrite", storage_options=storage_options, schema_mode="overwrite")

    print("Perfil guardado correctamente.")
    print(perfil_df.head())
    #--------------------------------------------------- Segundo llamado para obtener las ultimas 20 partidas del jugador cabe aclarar que viene los id_match que para cada uno hay q hacer un llamado

    endpoint= f"lol/match/v5/matches/by-puuid/{puuidUsuario}/ids"
    listaMatchs_json= get_data(url_base, endpoint,headers=headers) 

    #--------------------------------------------------- 20 Llamados a la api, ya que se extrayeron 20 id_match con esta linea hago 20 llamados para poder tener los datos de cada partida
    print("Cargando datos de las partidas(aprox: 20s )...")

    datos_acumulados = []
    for matchs in listaMatchs_json:
        endpoint= f"lol/match/v5/matches/{matchs}"
        match1 = get_data(url_base, endpoint,headers=headers)
        listaParticipantes=match1['info']['participants']
        fecha_creacion= match1['info']['gameCreation']
        queue_id= match1['info']['queueId']

        for jugador in listaParticipantes:
            if jugador['puuid'] == usuario_json['puuid']:
                jugador['gameCreation'] = fecha_creacion
                jugador['queueId'] = queue_id

                datos_acumulados.append(jugador)                      # esta linea es lo que almacena cada informacion por partida


    #---------------------------------------------------------------- se crea el dataframe y se procesa un poco porque alparecer tenia demasiada informacion para la libreria (fuente gemini(me rompia el codigo))

    df_final = pd.DataFrame(datos_acumulados)

    #df_final.to_excel("historial_completo_riot.xlsx", index=False)


    columnas_complejas = ['challenges', 'perks', 'styles']          #esto lo agregue porque al parecer la api entrega demasiada informacion osea un objeto entre listas de objetos que despues ese objeto tiene otra lista de objetos y me rompia el codigo
    for col in columnas_complejas:                                  #quiero mencionar que aca tuve un poco de ayuda de mi confiable amigo gemini
        if col in df_final.columns:
            df_final[col] = df_final[col].astype(str)



    #--------------------------------------------------------------- # Almacenamiento en minIO

    bronze_dir = f"s3://{bkt_name}/datalake/bronze/riotgames_api"
    stations_raw_dir = f"{bronze_dir}/registroSebastian"
    save_data_as_delta(df_final, stations_raw_dir, storage_options, mode="append")

    #--------------------------------------------------------------- Informacion para debug
    dt = DeltaTable(stations_raw_dir, storage_options=storage_options)
    print(f"Cant de filas: {dt.to_pandas().shape[0]}")

    print("Dataframe guardado en Delta Lake correctamente.")
    st.write("Partidas extraidas", dt.to_pandas().shape[0])
