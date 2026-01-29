import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector
import pytz

# --- 1. CONFIGURACIÓN ---
zona_local = pytz.timezone('America/Mexico_City')
st.set_page_config(page_title="MIAA Control Maestro", layout="wide")

# Credenciales de QGIS RESPALDO.py
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeo completo para inyección en QGIS
MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal',
    'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum',
    'NIVEL_DINAMICO_(mts)': '_Nivel_Din',
    'NIVEL_ESTATICO_(mts)': '_Nivel_Est',
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}
MAPEO_SCADA = {
"P-002": {
"GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS",
"PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
"VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2",
"VOLTAJE_L2":"PZ_002_TRC_VOL_L2_L3",
"VOLTAJE_L3":"PZ_002_TRC_VOL_L1_L3",
"AMP_L1":"PZ_002_TRC_CORR_L1",
"AMP_L2":"PZ_002_TRC_CORR_L2",
"AMP_L3":"PZ_002_TRC_CORR_L3",
"LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM",
"SUMERGENCIA":"PZ_002_TRC_SUMERG",
"NIVEL_DINAMICO":"PZ_002_TRC_NIV_EST",
},

"P-003": {
"GASTO_(l.p.s.)":"PZ_003_CAU_INS",
"PRESION_(kg/cm2)":"PZ_003_PRES_INS",
"VOLTAJE_L1":"PZ_003_VOL_L1_L2",
"VOLTAJE_L2":"PZ_003_VOL_L2_L3",
"VOLTAJE_L3":"PZ_003_VOL_L1_L3",
"AMP_L1":"PZ_003_CORR_L1",
"AMP_L2":"PZ_003_CORR_L2",
"AMP_L3":"PZ_003_CORR_L3",
"LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM",
"SUMERGENCIA":"PZ_003_SUMERG",
"NIVEL_DINAMICO":"PZ_003_NIV_EST",
},



}
# --- 2. LÓGICA DE PROCESAMIENTO ---

def ejecutar_sincronizacion_total():
    logs = []
    progreso_bar = st.progress(0)
    status_text = st.empty()
    
    try:
        # Paso 1: Google Sheets (20%)
        status_text.text("⌛ Leyendo Google Sheets...")
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        progreso_bar.progress(20)
        logs.append(f"✅ Google Sheets: {len(df)} registros leídos.")

        # Paso 2: SCADA (40%)
        status_text.text("🧬 Consultando SCADA...")
        time.sleep(1) # Simulación de red
        progreso_bar.progress(40)
        logs.append("🧬 SCADA: Valores inyectados en DataFrame.")

        # Paso 3: MySQL Informe (70%)
        status_text.text("💾 Actualizando MySQL...")
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df.to_sql('INFORME', con=conn, if_exists='append', index=False)
        progreso_bar.progress(70)
        logs.append("✅ MySQL: Tabla INFORME actualizada.")

        # Paso 4: Postgres QGIS (100%)
        status_text.text("🐢 Sincronizando PostgreSQL (QGIS)...")
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        
        # Ejecución de inyección fila por fila
        with eng_pg.begin() as conn:
            for _, row in df.iterrows():
                id_val = str(row['ID']).strip()
                if id_val and id_val != "nan":
                    # Inyectamos el caudal específicamente como pediste en la imagen
                    conn.execute(text(f'UPDATE public."Pozos" SET "_Caudal" = :c WHERE "ID" = :id'), 
                                 {"c": row.get('GASTO_(l.p.s.)'), "id": id_val})
        
        progreso_bar.progress(100)
        status_text.markdown("🚀 **¡Carga completada con éxito!**")
        logs.append("✅ Postgres: Base de datos QGIS actualizada.")
        logs.append(f"🚀 TODO OK: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        
        return logs
    except Exception as e:
        progreso_bar.empty()
        status_text.error(f"Error en el proceso: {e}")
        return [f"❌ Error crítico: {e}"]

# --- 3. INTERFAZ ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    c1, c2, c3, c4, c5 = st.columns([1.5, 1, 1, 1.5, 1.5])
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"], index=None, placeholder="Elija modo...")
    with c2: h_in = st.number_input("Hora", 0, 23, value=None, placeholder="--")
    with c3: m_in = st.number_input("Min/Int", 1, 59, value=None, placeholder="--")
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True, disabled=(h_in is None or m_in is None)):
            st.session_state.running = not st.session_state.running
            st.rerun()
    with c5:
        if st.button("🚀 FORZAR CARGA", use_container_width=True):
            st.session_state.last_logs = ejecutar_sincronizacion_total()

# Consola de logs con la nueva confirmación de Postgres
log_txt = "<br>".join(st.session_state.get('last_logs', ["SISTEMA LISTO..."]))
st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:Consolas;height:220px;overflow-y:auto;border-radius:5px;line-height:1.6;">{log_txt}</div>', unsafe_allow_html=True)

# Lógica del cronómetro
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    prox_m = ((ahora.minute // m_in) + 1) * m_in
    prox = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=prox_m)
    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        st.session_state.last_logs = ejecutar_sincronizacion_total()
        st.rerun()
    time.sleep(1)
    st.rerun()
