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
st.set_page_config(page_title="MIAA Data Center", layout="wide")

# Credenciales (Usando st.secrets para seguridad en la web)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeos exactos de tu respaldo
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS",
        "NIVEL_DINAMICO": "PZ_002_TRC_NIV_EST"
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal',
    'PRESION_(kg/cm2)': '_Presion',
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 2. LÓGICA DE TU RESPALDO ---

def limpiar_dato_para_postgres(valor):
    if pd.isna(valor) or valor == "" or str(valor).lower() == "nan": return None
    if isinstance(valor, str):
        v = valor.replace(',', '').strip()
        try: return float(v)
        except: return valor
    return valor

def ejecutar_actualizacion_maestra():
    try:
        # A. Leer Sheets
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        df['ID'] = df['ID'].astype(str).str.strip()

        # B. Inyección SCADA (Tu lógica prioritaria)
        conn_s = mysql.connector.connect(**DB_SCADA)
        cur_s = conn_s.cursor(dictionary=True)
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                cur_s.execute("SELECT h.VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME = %s AND h.FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY h.FECHA DESC LIMIT 1", (tag_name,))
                res = cur_s.fetchone()
                if res and res['VALUE'] is not None:
                    val_f = float(res['VALUE'])
                    if val_f > 0: df.loc[df['ID'] == p_id, col_excel] = round(val_f, 2)
        cur_s.close(); conn_s.close()

        # C. MySQL INFORME (Tu lógica de truncar e insertar)
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            db_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
            df[df.columns.intersection(db_cols)].to_sql('INFORME', con=conn, if_exists='append', index=False)

        # D. Postgres QGIS
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        with eng_pg.connect() as conn:
            with conn.begin():
                for _, row in df.iterrows():
                    id_m = str(row['ID']).strip()
                    if not id_m or id_m == "nan": continue
                    params = {"id": id_m}
                    sets = [f'"{c_pg}" = :{c_pg}' for c_ex, c_pg in MAPEO_POSTGRES.items() if c_ex in df.columns]
                    for c_ex, c_pg in MAPEO_POSTGRES.items():
                        if c_ex in df.columns: params[c_pg] = limpiar_dato_para_postgres(row[c_ex])
                    if sets: conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
        return True
    except: return False

# --- 3. TU DISEÑO DE INTERFAZ ORIGINAL ---
st.markdown("""<style>
    .main { background-color: #f0f2f6; }
    .stButton>button { border-radius: 5px; height: 3em; }
</style>""", unsafe_allow_html=True)

st.title("🖥️ MIAA Data Center")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4, c5 = st.columns([2, 1, 1, 2, 2])
    
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"], index=0)
    with c2: h_in = st.number_input("Hora", 0, 23, 8)
    with c3: m_in = st.selectbox("Min/Int", ["01", "05", "10", "15", "30", "58"], index=2)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR"
        btn_color = "primary" if not st.session_state.running else "secondary"
        if st.button(btn_label, use_container_width=True, type=btn_color):
            st.session_state.running = not st.session_state.running
            st.rerun()
    with c5:
        if st.button("🚀 FORZAR CARGA", use_container_width=True):
            ejecutar_actualizacion_maestra()

# Consola de texto (Tu diseño de terminal negra con letras verdes)
st.markdown('<div style="background-color: black; color: #00FF00; padding: 10px; font-family: Consolas; border-radius: 5px; height: 300px; overflow-y: scroll;">' + 
            f"SISTEMA LISTO - ESPERANDO ACCIÓN...<br>" + 
            (f"ESTADO: EJECUTANDO MONITOR (Modo {modo})..." if st.session_state.running else "ESTADO: DETENIDO") + 
            '</div>', unsafe_allow_html=True)

# Lógica de Segundero
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
        ejecutar_actualizacion_maestra()
        st.rerun()
    
    time.sleep(1)
    st.rerun()
