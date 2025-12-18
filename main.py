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

# --- EL FLUJO DEL PIPELINE ---
if __name__ == "__main__":
    print("🚀 INICIANDO PIPELINE DE DATOS: Player Analisis\n")

    st.title("🚀 Mi Proyecto de Data Engineering")
    st.write("Si puedes leer esto, ¡el servidor funciona!")
    
    if st.button("🚀 Correr script completo"):# 1. Ejecutar Bronze (Extracción)
        extraccion.extraccion_lolstats()
        print("✅ Extracción completada.\n")
            
            # 2. Ejecutar Silver (Limpieza)
            # Solo se ejecuta si el paso 1 (Bronze) fue True
        if transformacion.verificar_silver():
                st.dataframe(transformacion.partidas_silver())
                print("✨ Sin partidas nuevas para procesar. Pipeline terminado.")
        else:
                transformacion.ejecutar_transformacion()
                print("🎉 Nuevas partidas procesadas. Pipeline terminado.")
    

    if st.button("🔄 Buscar nuevas partidas"):
        print("-------------------------------")
        st.write("Buscando nuevas partidas...")
        extraccion.extraccion_lolstats()
        st.write("Proceso completado. Revisa la consola para más detalles.")

    if st.button("📊 Ver datos Silver"):
        st.write("Mostrando datos Silver...")
        transformacion.ejecutar_transformacion()
        