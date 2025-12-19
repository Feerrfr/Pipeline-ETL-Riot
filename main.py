import subprocess
import sys
import time
import streamlit as st
import TransformacionStats as transformacion
import Extraccion_lolstats as extraccion

@st.cache_data

# --- Preconfiguracion de Streamlit con fondo personalizado ---

def poner_fondo(url_imagen):
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("{url_imagen}");
            background-attachment: fixed;
            background-size: cover;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )

url_fondo = "https://cloudfront-us-east-1.images.arcpublishing.com/infobae/4QFZRNIDT5BVNNGLOUZ4SHORCE.jpg"
poner_fondo(url_fondo)

st.set_page_config(page_title="LoL Data Lake", page_icon="⚔️", layout="wide")

# --- EL FLUJO DEL PIPELINE ---

if __name__ == "__main__":
    print("🚀 INICIANDO PIPELINE DE DATOS: Player Analisis\n")

    st.title("🚀 Analisis a un jugador de League Of Legends ETL:")
    st.write("Panel de control")
    #st.dataframe(transformacion.transformar_partidas(transformacion.entregar_bronze()))


    id = st.text_input("Ingresa el Riot ID / Nombre:", placeholder="Ej: Faker")
    partes = id.split('#', 1)
        
    nick = partes[0].strip() # 'Sebax' (strip quita espacios en blanco sobrantes)
    tag = partes[1].strip()

    if st.button("🚀 Correr script completo"):# 1. Ejecutar Bronze (Extracción)
        extraccion.extraccion_lolstats(nick, tag)
        print("✅ Extracción completada.\n")
            
            # 2. Ejecutar Silver (Limpieza)
            # Solo se ejecuta si el paso 1 (Bronze) fue True
        transformacion.ejecutar_transformacion(nick, tag)
        print("✅ Transformación completada.\n")
    

    if st.button("🔄 Buscar nuevas partidas"):
        print("-------------------------------")
        st.write("Buscando nuevas partidas...")
        extraccion.extraccion_lolstats(nick, tag)
        st.write("Proceso completado. Revisa la consola para más detalles.")

    if st.button("📊 Ver datos Silver"):
        st.write("Mostrando datos Silver...")
        transformacion.ejecutar_transformacion(nick, tag)


