import subprocess
import sys
import time

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
    
    # 1. Ejecutar Bronze (Extracción)
    if ejecutar_script("Extraccion_lolstats.py"):
        
        # 2. Ejecutar Silver (Limpieza)
        # Solo se ejecuta si el paso 1 (Bronze) fue True
        if ejecutar_script("TransformacionStats.py"):
            
            print("✨ ¡PIPELINE COMPLETADO EXITOSAMENTE! ✨")
        else:
            print("💀 El proceso murió en la etapa Silver.")
    else:
        print("💀 El proceso murió en la etapa Bronze.")