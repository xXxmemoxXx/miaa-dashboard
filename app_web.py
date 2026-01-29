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

# --- 1. CONFIGURACIÓN ---
zona_local = pytz.timezone('America/Mexico_City')
st.set_page_config(page_title="MIAA Control Maestro", layout="wide")

# Credenciales fijas de tu respaldo
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

SHEET_ID = '1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw'
CSV_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeos completos restaurados
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
        "VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2",
        "AMP_L1":"PZ_002_TRC_CORR_L1",
        "LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM",
        "NIVEL_DINAMICO":"PZ_002_TRC_NIV_EST",
    },
    "P-003": {
        "GASTO_(l.p.s.)":"PZ_003_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_003_PRES_INS",
        "VOLTAJE_L1":"PZ_003_VOL_L1_L2",
        "AMP_L1":"PZ_003_CORR_L1",
        "LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM",
        "NIVEL_DINAMICO":"PZ_003_NIV_EST",
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal',
    'PRESION_(kg/cm2)': '_Presion',
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion',
}

# --- 2. LÓGICA DE ACTUALIZACIÓN CON ICONOS VERDES ---

def ejecutar_sincronizacion_total():
    resumen = []
    try:
        # Paso 1: Leer Google Sheets
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        resumen.append(f"✅ Google Sheets: {len(df)} registros leídos.")

        # Paso 2: SCADA
        conn_s = mysql.connector.connect(**DB_SCADA)
        cur_s = conn_s.cursor(dictionary=True)
        cambios = False
        for p_id, config in MAPEO_SCADA.items():
            for col, tag in config.items():
                cur_s.execute("SELECT VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME = %s ORDER BY h.FECHA DESC LIMIT 1", (tag,))
                res = cur_s.fetchone()
                if res and float(res['VALUE']) > 0:
                    df.loc[df['POZOS'] == p_id, col] = round(float(res['VALUE']), 2)
                    cambios = True
        cur_s.close(); conn_s.close()
        resumen.append("🧬 SCADA: Valores inyectados en DataFrame.")

        # Paso 3: Guardar en Nube (Google Sheets)
        if cambios:
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=["https://www.googleapis.com/auth/spreadsheets"])
            gc = gspread.authorize(creds).open_by_key(SHEET_ID).worksheet("informe")
            gc.update('A1', [df.fillna('').columns.values.tolist()] + df.fillna('').values.tolist())
        
        # Paso 4: MySQL Informe
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df.to_sql('INFORME', con=conn, if_exists='append', index=False)
        resumen.append("💾 MySQL: Tabla INFORME actualizada.")

        # Paso 5: Postgres QGIS
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        with eng_pg.begin() as conn:
            for _, row in df.iterrows():
                id_val = str(row['ID']).strip()
                if id_val and id_val != "nan":
                    conn.execute(text(f'UPDATE public."Pozos" SET "_Caudal" = :c WHERE "ID" = :id'), {"c": row.get('GASTO_(l.p.s.)'), "id": id_val})
        resumen.append("🐢 Sincronizando PostgreSQL (QGIS)...")
        
        resumen.append(f"🚀 TODO OK: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        return resumen
    except Exception as e:
        return [f"❌ Error crítico: {str(e)}"]

# --- 3. INTERFAZ ---
st.title("🖥️ MIAA")

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

# Consola con iconos verdes
log_txt = "<br>".join(st.session_state.get('last_logs', ["ESPERANDO ACCIÓN..."]))
st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:Consolas;height:200px;overflow-y:auto;border-radius:5px;line-height:1.5;">{log_txt}</div>', unsafe_allow_html=True)

if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    # Lógica de segunderos
    prox_m = ((ahora.minute // m_in) + 1) * m_in
    prox = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=prox_m)
    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        st.session_state.last_logs = ejecutar_sincronizacion_total()
        st.rerun()
    time.sleep(1)
    st.rerun()

