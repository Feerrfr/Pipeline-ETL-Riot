import pandas as pd
from deltalake import DeltaTable, write_deltalake
from configparser import ConfigParser
from datetime import datetime
from pprint import pprint
import streamlit as st

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
def partidas_silver():
    dt_silver = DeltaTable(statsSilver_dir, storage_options=storage_options)
    return dt_silver.to_pandas()

#---- Configuracion de parser

parser = ConfigParser()                                             # Sirve para entrar a pipeline.conf y sacar las credenciales de ahi
parser.optionxform = str
parser.read("pipeline.conf")

storage_options = dict(st.secrets["mysql-db"])
bkt_name = storage_options["bkt_name"]



#---- Lectura de Delta y convertido en dataframe

bronze_dir = f"s3://{bkt_name}/datalake/bronze/riotgames_api"
statsBronze_dir = f"{bronze_dir}/registroSebastian"

silver_dir = f"s3://{bkt_name}/datalake/silver/riotgames_api"
statsSilver_dir = f"{silver_dir}/registroSebastian"

dt_bronze = DeltaTable(statsBronze_dir, storage_options=storage_options)


def entregar_bronze():
    return dt_bronze.to_pandas()
df = dt_bronze.to_pandas()
#print(df.dtypes)
partidas_guardadas = 0
#---- Verificar existencia de silver_data, y obtener la última fecha procesada. De no haber nueva el script se detiene.
def verificar_silver():
    print("Última partida procesada...")

    ultima_fecha = 0
    partidas_guardadas = 0
    try:
        # 1. Intentamos conectar a la tabla Silver existente
        dt_silver = DeltaTable(statsSilver_dir, storage_options=storage_options)
        df_fechas = dt_silver.to_pandas(columns=["gameCreation"])
        partidas_guardadas = len(df_fechas)

        if not df_fechas.empty:
            # Buscamos el valor máximo (la partida más reciente)
            ultima_fecha = df_fechas["gameCreation"].max()
            print(f"📅 Última fecha encontrada en Silver: {datetime.fromtimestamp(ultima_fecha / 1000)}")
            return True
        else:
            print("⚠️ La tabla Silver existe pero está vacía. Se procesará todo.") 
            return True  
    except Exception as e:
        print("✨ No existe tabla Silver aún (Carga Inicial). Se procesará todo.")
        print(f"Detalle del error: {e}")

    df = df[df['gameCreation'] > ultima_fecha].copy()
    cantidad_nuevas = len(df)
    print(f"🔥 Nuevas partidas para procesar: {cantidad_nuevas}")
    if df.empty:
        print("😴 No hay datos nuevos. El script termina aquí.")
        print(f"Partidas totales en silver: {partidas_guardadas}.")
        return False

#---- 1era Transformacion borrar las partidas no interesantes, que en este caso son las que no son clasificatorias, estas se identifican con queueId
df_silver = df
def transformar_partidas(df):
    basura = ["NONE", "nan", "null", "NA", ""]
    df = df[~df["lane"].isin(basura)]

    print(df.info( memory_usage= 'deep'))

    ids_rankeds = [420, 440]
    df = df[df["queueId"].isin(ids_rankeds)]


    #---- 2era Transformacion ordenar tipos de datos por fecha
    df_silver = df
    df_silver = df_silver.sort_values(by='gameCreation', ascending=False).reset_index(drop=True) # Se ordenan por ascendente y se resetean los indices


    df_silver['fecha_hora'] = pd.to_datetime(df_silver['gameCreation'], unit='ms', utc=False)   # Conversion de timestamp a fecha y hora
    hora_argentina = df_silver['fecha_hora'].dt.tz_localize('UTC').dt.tz_convert('America/Argentina/Buenos_Aires')
    df_silver['fecha_local'] = hora_argentina.dt.tz_localize(None)

    muertes_seguras = df_silver['deaths'].replace(0, 1)
    df_silver['kda'] = (df_silver['kills'] + df_silver['assists']) / muertes_seguras
    df_silver['kda'] = df_silver['kda'].round(2)

    #---- 3da Transformacion seleccionar columnas de interes
    columnas_elegidas = [
        'fecha_local',
        'championName',
        'lane',
        'kda',
        'win',
        'kills',
        'deaths',
        'assists',
        'totalDamageDealtToChampions',
        'goldEarned',
        'visionScore',
        'totalMinionsKilled',
        'item0',
        'item1',
        'item2',
        'item3',
        'item4',
        'item5',
        'item6',
        'gameCreation'
    ]

    df_silver = df_silver[columnas_elegidas].copy()

    #----4ta Transformacion cambio de tipo de variables
    conversion_mapping = {
        "fecha_local": "datetime64[ns]",
        "championName": "string",
        "lane": "string",
        "kda": "float32",
        "win": "bool",
        "kills": "int8",
        "deaths": "int8",
        "assists": "int8",
        "totalDamageDealtToChampions": "int32",
        "goldEarned": "int32",
        "visionScore": "int8",
        "totalMinionsKilled": "int16",
        "item0": "int16",
        "item1": "int16",
        "item2": "int16",
        "item3": "int16",
        "item4": "int16",
        "item5": "int16",
        "item6": "int16"
        }

    df_silver = df_silver.astype(conversion_mapping)


    #---- 5ta Transformacion borrar duplicados
    df_silver = df_silver.drop_duplicates().reset_index(drop=True)
    return df_silver


def guardar_silver(df_silver):
    print(df_silver.head())
    print(df_silver.info(memory_usage= 'deep'))

    cantidad_filas_nuevas= len(df_silver)
    save_data_as_delta(df_silver, statsSilver_dir, storage_options, mode="append")
    print(f"Partidas antiguas en silver: {partidas_guardadas}, nuevas: {cantidad_filas_nuevas}.")
    st.dataframe(df_silver)                 #muestra en streamlit el dataframe silver


def ejecutar_transformacion():
    if verificar_silver():
        df_silver = transformar_partidas(entregar_bronze())
        guardar_silver(df_silver)