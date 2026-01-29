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
st.set_page_config(page_title="MIAA Data Center", layout="wide")

# Credenciales de Bases de Datos (Fieles a tu archivo)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

# Configuración Google Sheets
SHEET_ID = '1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw'
CSV_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeo idéntico al respaldo
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

# --- 2. LÓGICA DE ACTUALIZACIÓN ---

def ejecutar_sincronizacion_total():
    logs = []
    try:
        # A. Leer Hoja Actual
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        df['ID'] = df['ID'].astype(str).str.strip()

        # B. Consultar SCADA con Regla > 0
        conn_s = mysql.connector.connect(**DB_SCADA)
        cur_s = conn_s.cursor(dictionary=True)
        hay_cambios_scada = False

        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                cur_s.execute("""
                    SELECT h.VALUE FROM vfitagnumhistory h 
                    JOIN VfiTagRef r ON h.GATEID = r.GATEID 
                    WHERE r.NAME = %s AND h.FECHA >= NOW() - INTERVAL 1 HOUR 
                    ORDER BY h.FECHA DESC LIMIT 1
                """, (tag_name,))
                res = cur_s.fetchone()
                
                if res and res['VALUE'] is not None:
                    val_f = float(res['VALUE'])
                    # REGLA OBLIGATORIA: SOLO SI ES MAYOR A 0
                    if val_f > 0:
                        df.loc[df['ID'] == p_id, col_excel] = round(val_f, 2)
                        hay_cambios_scada = True
                        logs.append(f"SCADA: {p_id} -> {val_f} detectado (OK)")
                    else:
                        logs.append(f"SCADA: {p_id} reportó 0.0 o inferior. SE IGNORA.")

        cur_s.close(); conn_s.close()

        # C. Escribir en Google Sheets (Solo si hubo datos válidos)
        if hay_cambios_scada:
            scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(SHEET_ID).worksheet("informe")
            df_limpio = df.fillna('')
            sheet.update('A1', [df_limpio.columns.values.tolist()] + df_limpio.values.tolist())
            logs.append("GOOGLE SHEETS: Actualizado con valores SCADA.")

        # D. MySQL (Tabla INFORME)
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            cols_db = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
            df[df.columns.intersection(cols_db)].to_sql('INFORME', con=conn, if_exists='append', index=False)
        logs.append("MYSQL: Tabla INFORME actualizada.")

        # E. Postgres (QGIS)
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        with eng_pg.connect() as conn:
            with conn.begin():
                for _, row in df.iterrows():
                    id_m = str(row['ID']).strip()
                    params = {"id": id_m}
                    sets = [f'"{c_pg}" = :{c_pg}' for c_ex, c_pg in MAPEO_POSTGRES.items() if c_ex in df.columns]
                    for c_ex, c_pg in MAPEO_POSTGRES.items():
                        if c_ex in df.columns:
                            val = row[c_ex]
                            params[c_pg] = None if pd.isna(val) or val == "" else val
                    if sets:
                        conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
        logs.append("POSTGRES: Capa QGIS actualizada.")
        return logs
    except Exception as e:
        return [f"ERROR: {str(e)}"]

# --- 3. INTERFAZ (TU DISEÑO ORIGINAL) ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 2, 2])
    
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"], index=0)
    with c2: h_in = st.number_input("Hora", 0, 23, 8)
    with c3: m_in = st.selectbox("Min/Int", ["01", "05", "10", "15", "30", "58"], index=2)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR"
        if st.button(btn_label, use_container_width=True, type="primary" if not st.session_state.running else "secondary"):
            st.session_state.running = not st.session_state.running
            st.rerun()
    with c5:
        if st.button("🚀 FORZAR CARGA MANUAL", use_container_width=True):
            resultados = ejecutar_sincronizacion_total()
            st.session_state.last_logs = resultados

# Consola Terminal Negra/Verde
log_content = "<br>".join(st.session_state.get('last_logs', ["SISTEMA LISTO..."]))
st.markdown(f'''
    <div style="background-color: black; color: #00FF00; padding: 15px; font-family: 'Consolas', monospace; border-radius: 5px; height: 250px; overflow-y: auto; border: 2px solid #333;">
        {log_content}
    </div>
''', unsafe_allow_html=True)

# Lógica de Segundero y Bucle
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    m_val = int(m_in)
    
    if modo == "Diario":
        prox = ahora.replace(hour=h_in, minute=m_val, second=0, microsecond=0)
        if prox <= ahora: prox += datetime.timedelta(days=1)
    else:
        prox_m = ((ahora.minute // m_val) + 1) * m_val
        if prox_m >= 60:
            prox = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        else:
            prox = ahora.replace(minute=prox_m, second=0, microsecond=0)
    
    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        st.session_state.last_logs = ejecutar_sincronizacion_total()
        st.rerun()
    
    time.sleep(1)
    st.rerun()
