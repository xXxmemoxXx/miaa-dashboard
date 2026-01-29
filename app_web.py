import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector
import pytz
try:
    import gspread
    from google.oauth2.service_account import Credentials
except ImportError:
    st.error("Por favor añade 'gspread' y 'google-auth' a tu archivo requirements.txt")

# --- 1. CONFIGURACIÓN (Fiel a QGIS RESPALDO.py) ---
zona_local = pytz.timezone('America/Mexico_City')
st.set_page_config(page_title="MIAA Control Maestro", layout="wide")

DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

SHEET_ID = '1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw'
CSV_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=informe'

MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS"
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal',
    'PRESION_(kg/cm2)': '_Presion',
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 2. FUNCIONES (Integrando tu lógica de respaldo) ---

def limpiar_dato(v):
    if pd.isna(v) or v == "" or str(v).lower() == "nan": return None
    if isinstance(v, str):
        v = v.replace(',', '').strip()
        try: return float(v)
        except: return v
    return v

def ejecutar_sincronizacion():
    logs = []
    try:
        df = pd.read_csv(CSV_URL)
        df.columns = [c.strip().replace('\n', ' ') for c in df.columns]
        df['ID'] = df['ID'].astype(str).str.strip()

        # A. Consulta SCADA y Validación > 0
        conn_s = mysql.connector.connect(**DB_SCADA)
        cur_s = conn_s.cursor(dictionary=True)
        cambios = False
        
        for p_id, config in MAPEO_SCADA.items():
            for col, tag in config.items():
                cur_s.execute("SELECT VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME = %s AND h.FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY h.FECHA DESC LIMIT 1", (tag,))
                res = cur_s.fetchone()
                if res and res['VALUE'] is not None:
                    val = float(res['VALUE'])
                    if val > 0: # REGLA SOLICITADA
                        df.loc[df['ID'] == p_id, col] = round(val, 2)
                        cambios = True
                        logs.append(f"✓ {p_id}: {val} inyectado.")
                    else:
                        logs.append(f"⚠ {p_id}: Valor 0 omitido.")
        cur_s.close(); conn_s.close()

        # B. Actualizar Google Sheets (Si hay valores > 0)
        if cambios:
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/spreadsheets"])
            client = gspread.authorize(creds)
            sheet = client.open_by_key(SHEET_ID).worksheet("informe")
            sheet.update('A1', [df.fillna('').columns.values.tolist()] + df.fillna('').values.tolist())
            logs.append("✓ Google Sheets actualizado.")

        # C. MySQL y Postgres (Lógica idéntica a tu respaldo)
        engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{urllib.parse.quote_plus(DB_INFORME['password'])}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with engine_my.begin() as c:
            c.execute(text("TRUNCATE TABLE INFORME"))
            cols = [r[0] for r in c.execute(text("SHOW COLUMNS FROM INFORME"))]
            df[df.columns.intersection(cols)].to_sql('INFORME', con=c, if_exists='append', index=False)
        
        engine_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{urllib.parse.quote_plus(DB_POSTGRES['pass'])}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        with engine_pg.begin() as c:
            for _, row in df.iterrows():
                params = {"id": str(row['ID']).strip()}
                sets = [f'"{pg}" = :{pg}' for ex, pg in MAPEO_POSTGRES.items() if ex in df.columns]
                for ex, pg in MAPEO_POSTGRES.items():
                    if ex in df.columns: params[pg] = limpiar_dato(row[ex])
                if sets: c.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
        
        logs.append("✓ Bases de Datos sincronizadas.")
        return logs
    except Exception as e:
        return [f"❌ ERROR: {str(e)}"]

# --- 3. INTERFAZ (Tu diseño original) ---
st.title("🖥️ MIAA Control Maestro")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 2, 2])
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora", 0, 23, 8)
    with c3: m_in = st.selectbox("Min/Int", ["01", "05", "10", "15", "30", "58"], index=2)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True, type="primary" if not st.session_state.running else "secondary"):
            st.session_state.running = not st.session_state.running
            st.rerun()
    with c5:
        if st.button("🚀 CARGA MANUAL", use_container_width=True):
            st.session_state.last_logs = ejecutar_sincronizacion()

# Consola Terminal (Negro/Verde como en el respaldo)
log_txt = "<br>".join(st.session_state.get('last_logs', ["SISTEMA INICIADO..."]))
st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:Consolas;height:200px;overflow-y:auto;border-radius:5px;">{log_txt}</div>', unsafe_allow_html=True)

if st.session_state.running:
    # Lógica de temporizador igual a tu archivo .py
    time.sleep(1)
    st.rerun()
