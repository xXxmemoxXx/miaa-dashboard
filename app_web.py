import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import pytz  # Librería para manejar zonas horarias

# --- CONFIGURACIÓN DE ZONA HORARIA ---
zona_local = pytz.timezone('America/Mexico_City')

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA Control Center", layout="wide")

# --- 1. CONFIGURACIÓN DE CONEXIONES (Secrets) ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 2. MAPEOS (De tu respaldo original) ---
MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 3. FUNCIÓN DE EJECUCIÓN ---
def ejecutar_actualizacion():
    try:
        with st.status("🔄 Sincronizando...", expanded=True):
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            
            # MySQL
            pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
            engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with engine_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                res_cols = conn.execute(text("SHOW COLUMNS FROM INFORME"))
                db_cols = [r[0] for r in res_cols]
                df[[c for c in df.columns if c in db_cols]].to_sql('INFORME', con=conn, if_exists='append', index=False)
            
            st.toast("✅ Sincronización Exitosa", icon="🚀")
    except Exception as e:
        st.error(f"Error: {e}")

# --- 4. INTERFAZ ---
st.title("🖥️ MIAA Monitor Web")

with st.container(border=True):
    st.subheader("Configuración de Tiempo (Hora México)")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: hora_in = st.number_input("Hora (0-23)", 0, 23, 12)
    with c3: min_in = st.number_input("Minuto (0-59)", 0, 59, 51)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_txt = "🛑 PARAR MONITOR" if st.session_state.running else "▶️ INICIAR MONITOR"
        if st.button(btn_txt, use_container_width=True, type="primary" if st.session_state.running else "secondary"):
            st.session_state.running = not st.session_state.running
            st.rerun()

# --- 5. LÓGICA DE TIEMPO CON CORRECCIÓN DE ZONA ---
if st.session_state.running:
    # Obtener hora actual de México, no del servidor
    ahora = datetime.datetime.now(zona_local)
    
    if modo == "Diario":
        proximo = ahora.replace(hour=int(hora_in), minute=int(min_in), second=0, microsecond=0)
        if proximo <= ahora:
            proximo += datetime.timedelta(days=1)
    else:
        intervalo = int(min_in) if int(min_in) > 0 else 1
        prox_m = ((ahora.minute // intervalo) + 1) * intervalo
        if prox_m >= 60:
            proximo = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        else:
            proximo = ahora.replace(minute=prox_m, second=0, microsecond=0)

    diff = proximo - ahora
    
    # Mostrar la hora actual para confirmar que coincide con tu reloj
    st.write(f"🕒 Hora actual en México: **{ahora.strftime('%H:%M:%S')}**")
    st.metric("⏳ PRÓXIMA CARGA EN:", f"{str(diff).split('.')[0]}")

    if diff.total_seconds() <= 5:
        ejecutar_actualizacion()
        time.sleep(10)
        st.rerun()

    time.sleep(1)
    st.rerun()
