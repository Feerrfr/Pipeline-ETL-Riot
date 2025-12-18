import subprocess
import sys
import time
import streamlit as st
import TransformacionStats as transformacion
import Extraccion_lolstats as extraccion

@st.cache_data

def ejecutar_script(nombre_script):
    print(f"▶️ Iniciando: {nombre_script}...")
    inicio = time.time()
    
    resultado = subprocess.run([sys.executable, nombre_script], capture_output=False)
    
    fin = time.time()
    duracion = round(fin - inicio, 2)
    
    if resultado.returncode == 0:
        print(f"✅ Éxito: {nombre_script} terminó bien en {duracion} seg.\n")
        return True
    else:
        print(f"❌ Error: {nombre_script} falló. Deteniendo el pipeline.")
        return False
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
    st.dataframe(transformacion.transformar_partidas(transformacion.entregar_bronze()))


    if st.button("🚀 Correr script completo"):# 1. Ejecutar Bronze (Extracción)
        extraccion.extraccion_lolstats()
        print("✅ Extracción completada.\n")
            
            # 2. Ejecutar Silver (Limpieza)
            # Solo se ejecuta si el paso 1 (Bronze) fue True
        transformacion.ejecutar_transformacion()
        print("✅ Transformación completada.\n")
    

    if st.button("🔄 Buscar nuevas partidas"):
        print("-------------------------------")
        st.write("Buscando nuevas partidas...")
        extraccion.extraccion_lolstats()
        st.write("Proceso completado. Revisa la consola para más detalles.")

    if st.button("📊 Ver datos Silver"):
        st.write("Mostrando datos Silver...")
        transformacion.ejecutar_transformacion()
        