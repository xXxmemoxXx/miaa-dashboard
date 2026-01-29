import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector
import pytz
import gspread
from google.oauth2.service_account import Credentials

# --- CONFIGURACIÓN ---
zona_local = pytz.timezone('America/Mexico_City')
st.set_page_config(page_title="MIAA Control Maestro", layout="wide")

# Credenciales desde Secrets (Fieles a tu respaldo)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
SHEET_ID = '1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw'
CSV_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=informe'

# --- LÓGICA DE PROCESAMIENTO ---
def ejecutar_sincronizacion_total():
    logs = []
    try:
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        df['ID'] = df['ID'].astype(str).str.strip()

        conn_s = mysql.connector.connect(**DB_SCADA)
        cur_s = conn_s.cursor(dictionary=True)
        hay_cambio = False
        
        # Validación de valor > 0 para P-002
        cur_s.execute("SELECT VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME = 'PZ_002_TRC_CAU_INS' ORDER BY h.FECHA DESC LIMIT 1")
        res = cur_s.fetchone()
        
        if res and float(res['VALUE']) > 0:
            val_f = round(float(res['VALUE']), 2)
            df.loc[df['ID'] == 'P-002', 'GASTO_(l.p.s.)'] = val_f
            hay_cambio = True
            logs.append(f"✓ SCADA detectó {val_f}. Actualizando sistemas...")
        else:
            logs.append("⚠ SCADA reportó 0 o nulo. Se mantiene valor de la hoja.")

        cur_s.close(); conn_s.close()

        if hay_cambio:
            # 1. Escribir en Google Sheets
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/spreadsheets"])
            client = gspread.authorize(creds)
            sheet = client.open_by_key(SHEET_ID).worksheet("informe")
            sheet.update('A1', [df.fillna('').columns.values.tolist()] + df.fillna('').values.tolist())
            
            # 2. MySQL INFORME
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                df.to_sql('INFORME', con=conn, if_exists='append', index=False)
            
            logs.append("✓ Google Sheets y MySQL actualizados.")
        return logs
    except Exception as e:
        return [f"❌ Error: {str(e)}"]

# --- INTERFAZ CON SEGUNDEROS ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    c1, c2, c3, c4 = st.columns([2, 1, 1, 2])
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora", 0, 23, 8)
    with c3: m_in = st.selectbox("Min/Int", ["01", "05", "10", "15", "30", "58"], index=2)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
            st.session_state.running = not st.session_state.running
            st.rerun()

# Consola Verde
log_txt = "<br>".join(st.session_state.get('last_logs', ["ESPERANDO CARGA..."]))
st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:Consolas;height:150px;overflow-y:auto;border-radius:5px;">{log_txt}</div>', unsafe_allow_html=True)

# LÓGICA DE SEGUNDEROS (Tu reloj original)
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    m_val = int(m_in)
    if modo == "Diario":
        prox = ahora.replace(hour=h_in, minute=m_val, second=0, microsecond=0)
        if prox <= ahora: prox += datetime.timedelta(days=1)
    else:
        prox_m = ((ahora.minute // m_val) + 1) * m_val
        prox = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=prox_m) if prox_m < 60 else ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    
    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        st.session_state.last_logs = ejecutar_sincronizacion_total()
        st.rerun()
    
    time.sleep(1)
    st.rerun()
