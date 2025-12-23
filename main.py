import subprocess
import sys
import time
import streamlit as st
import TransformacionStats as transformacion
import Extraccion_lolstats as extraccion
import gold

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
    st.title("🐋 Pipeline ETL - Análisis de Jugadores de League Of Legends")
    nick = ""
    tag = ""   
    with st.form("mi_formulario"):
        st.write("Configuración")
        riot_id_input = st.text_input("Ingresa Riot ID (Nombre#Tag):", placeholder="Ej: Sebax#100")
        
        # Este botón es especial, no deja salir nada hasta que se aprieta
        enviado = st.form_submit_button("🕵️ Buscar Jugador")

        if enviado:
            # Aquí adentro pones la misma lógica de validación de arriba
            if "#" in riot_id_input:
                partes = riot_id_input.split('#', 1)
                nick = partes[0].strip()
                tag = partes[1].strip()
                st.session_state['usuario_nick'] = nick
                st.session_state['usuario_tag'] = tag
                #extraccion.extraccion_lolstats(nick, tag)
            else:
                st.error("Formato incorrecto.")
    #st.title("🚀 Analisis a un jugador de League Of Legends ETL:")
    #st.write("Panel de control")
    #st.dataframe(transformacion.transformar_partidas(transformacion.entregar_bronze()))


    #id = st.text_input("Ingresa el Riot ID / Nombre:", placeholder="Ej: Faker")
    #partes = id.split('#', 1)
        
    #nick = partes[0].strip() # 'Sebax' (strip quita espacios en blanco sobrantes)
    #tag = partes[1].strip()
    nick = st.session_state.get('usuario_nick')
    tag = st.session_state.get('usuario_tag')
    if nick is not None and tag is not None:
        
        st.info(f"Datos listos para usar: {nick} #{tag}")
        
        
        if st.button("🚀 Correr script completo"):# 1. Ejecutar Bronze (Extracción)
            print("-------------------------------")
            print("Iniciando proceso completo de ETL...\n")             
                # 1. Ejecutar Bronze (Extracción)
            extraccion.extraccion_lolstats(nick, tag)
            print("✅ Extracción completada.\n")
                
                # 2. Ejecutar Silver (Limpieza)
                # Solo se ejecuta si el paso 1 (Bronze) fue True
            transformacion.ejecutar_transformacion(nick, tag)
            print("✅ Transformación completada.\n")

                # 3. Ejecutar Gold (Análisis)
            gold.ejecutar_gold(nick, tag)
            print("✅ Análisis completado.\n")
        

        if st.button("🔄 Buscar nuevas partidas"):
                print("-------------------------------")
                st.write("Buscando nuevas partidas...")
                extraccion.extraccion_lolstats(nick, tag)
                st.write("Proceso completado. Revisa la consola para más detalles.")

        if st.button("📊 Ver datos Silver"):
            st.write("Ejecutando transformación y mostrando datos Silver...")
            transformacion.ejecutar_transformacion(nick, tag)

        if st.button("🐊 Analizar Gold"):
            st.title("Análisis Avanzado - Gold 📊")
            st.write("Ejecutando gold para análisis avanzado...")
            gold.ejecutar_gold(nick, tag)


    else:
    # Si son None, significa que el usuario aún no llenó el formulario
        st.write("👈 Por favor, completa el formulario y dale a 'Guardar' primero.")
    

