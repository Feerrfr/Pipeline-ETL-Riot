import streamlit as st
import pandas as pd
import deltalake
import plotly.express as px


storage_options = dict(st.secrets["mysql-db"])
bkt_name = storage_options["bkt_name"]

silver_dir = f"s3://{bkt_name}/datalake/silver/riotgames_api"
statsSilver_dir = ""

def setear_dir(nick, tag):
    statsSilver_dir = f"{silver_dir}/registro{nick}{tag}"
    return statsSilver_dir

def extraer_silver(nick, tag):
    rutaSilver = setear_dir(nick, tag)
    try:
        dt_silver = deltalake.DeltaTable(rutaSilver, storage_options=storage_options)
        df_silver = dt_silver.to_pandas()
        return df_silver
    
    except Exception as e:
        print("No existe tabla Silver aún.")
        print(f"Detalle del error: {e}")
        return pd.DataFrame()  # Retorna un DataFrame vacío si no existe la tabla Silver



def calcular_wr (df_silver):
    df_partidas = df_silver["win"]
    df_wins = df_partidas[df_partidas]
    wr = (len(df_wins) / len (df_partidas)) * 100
    print(df_partidas)
    print(f"Partidas {len(df_partidas)}, Win Rate: {wr:.2f}%")

    df_filtrado = df_partidas.iloc[5:]
    df_wins_filtrado = df_filtrado[df_filtrado]
    wr_filtrado = (len(df_wins_filtrado) / len(df_filtrado)) * 100
    varWr = wr - wr_filtrado
    print(f"Win Rate sin las primeras 5 partidas: {wr_filtrado:.2f}%, Variación: {varWr:.2f}%")



    return wr, varWr

def calcular_kda(df_silver):
    df_partidas = df_silver[["kills","deaths","assists"]]
    total_kills = df_partidas["kills"].sum()
    total_deaths = df_partidas["deaths"].sum()
    total_assists = df_partidas["assists"].sum()
    if total_deaths == 0:
        total_deaths = 1  # Evitar división por cero
    kda = (total_kills + total_assists) / total_deaths
    print(f"KDA Global: {kda:.2f}")

    df_filtrado = df_partidas.iloc[5:]
    kills_filtrado = df_filtrado["kills"].sum()
    deaths_filtrado = df_filtrado["deaths"].sum()
    assists_filtrado = df_filtrado["assists"].sum()
    if deaths_filtrado == 0:
        deaths_filtrado = 1  # Evitar división por cero
    kda_filtrado = (kills_filtrado + assists_filtrado) / deaths_filtrado
    varKda = kda - kda_filtrado
    return kda , varKda

def calcular_visionPromedio(df_silver):
    df_partidas = df_silver["visionScore"]
    vision_promedio = df_partidas.sum() / len(df_partidas)
    print(f"Vision Score Promedio: {vision_promedio:.2f}")

    df_filtrado = df_partidas.iloc[5:]
    vision_promedio_filtrado = df_filtrado.sum() / len(df_filtrado) 
    varVision = vision_promedio - vision_promedio_filtrado
    return vision_promedio, varVision

def armar_columnas(kda,varKda,wr,varWr,vision_promedio,varVision):
    col1, col2, col3 = st.columns(3)
    col1.metric("Winrate Promedio", f"{wr:.2f}%", f"{varWr:.2f}%")  
    col2.metric("KDA Promedio", f"{kda:.2f}%", f"{varKda:.2f}%")  
    col3.metric("Vision Score Promedio", f"{vision_promedio:.2f}", f"{varVision:.2f}") 
def df_poolchamp(df_silver):
    grupos = df_silver.groupby('championName')
    instrucciones = {
    'kills': 'sum',      # Suma las kills
    'deaths': 'sum',     # Suma las muertes
    'win': 'sum',        # Suma las victorias (True=1)
    'fecha_local': 'count'    # Cuenta cuántas partidas hay (usando el ID)
    }
    resumen = grupos.agg(instrucciones).reset_index()
    #print(resumen)
    resumen['winrate'] = ((resumen['win'] / resumen['fecha_local']) * 100).round(1)
    resumen['kda'] = (resumen['kills'] / resumen['deaths']).round(2)
    #print(resumen)
    columnas_elegidas = [
        'fecha_local',
        'championName',
        'winrate',
        'kda'
        ]
    df_poolchamp = resumen[columnas_elegidas].sort_values(by='fecha_local', ascending=False).reset_index(drop=True)
    #print(df_poolchamp)
    df_final = df_poolchamp.rename(columns ={'fecha_local': 'Partidas Jugadas', 'championName': 'Campeón', 'winrate': 'Winrate (%)', 'kda': 'KDA'})
    return df_final

def imprimir_analisisdf(df):
    col_izq, col_der = st.columns([1, 2]) # Columna izquierda más chica

    with col_izq:
        st.subheader("📊 Tabla de Posiciones")
        st.dataframe(df)

    with col_der:
        st.subheader("🔥 Partidas Jugadas por Campeón")
        df_grafico = df.sort_values(by='Partidas Jugadas', ascending=True)
        fig2 = px.bar(
            df_grafico,  # Ordenamos para que el más jugado quede arriba
            x='Partidas Jugadas',      # Largo de la barra
            y='Campeón',               # Etiquetas del costado
            orientation='h',           # 'h' = Horizontal
            
            # --- EXTRAS VISUALES ---
            text_auto=True,            # Pone el numerito automáticamente en la barra
            color='Winrate (%)',        # Pinta la barra según qué tanto ganas
            color_continuous_scale='RdYlGn', # Rojo=Alto WR, Azul=Bajo (o usa 'Viridis', 'Blues')
            
            # Datos extra al pasar el mouse
            #hover_data=['KDA', 'Win Rate %'] 
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    st.subheader("🗺️ Tu Territorio de Campeones")

    # El Treemap necesita que definas qué determina el tamaño y el color
    fig = px.treemap(
        df, 
        path=['Campeón'],          # Las etiquetas
        values='Partidas Jugadas', # El tamaño del cuadrado
        color='Winrate (%)',       # El color
        color_continuous_scale='RdYlGn', # Red-Blue (Rojo bajo, Azul alto)
        hover_data=['KDA']         # Dato extra al pasar el mouse
    )

    st.plotly_chart(fig, use_container_width=True)

if __name__ == "__main__":
    st.set_page_config(page_title="LoL Data Lake", page_icon="⚔️", layout="wide")
    print("Probando TransformacionStats.py")
    nick = "Sebax"
    tag = "100"
    df_silver = extraer_silver(nick, tag)
    if not df_silver.empty:
        wr, varWr = calcular_wr(df_silver)
        kda, varKda = calcular_kda (df_silver)
        vision_promedio, varVision = calcular_visionPromedio (df_silver)
        armar_columnas(kda, varKda, wr, varWr, vision_promedio, varVision) 
        imprimir_analisisdf(df_poolchamp (df_silver))
    else:
        st.warning("No hay datos en Silver para mostrar. Ejecuta el pipeline ETL primero.")

def ejecutar_gold(nick,tag):
    df_silver = extraer_silver(nick, tag)
    if not df_silver.empty:
        wr, varWr = calcular_wr(df_silver)
        kda, varKda = calcular_kda (df_silver)
        vision_promedio, varVision = calcular_visionPromedio (df_silver)
        armar_columnas(kda, varKda, wr, varWr, vision_promedio, varVision) 
        imprimir_analisisdf(df_poolchamp (df_silver))
    else:
        st.warning("No hay datos en Silver para analisar. Ejecuta el pipeline ETL primero.") 