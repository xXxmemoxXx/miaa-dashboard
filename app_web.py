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

# Credenciales (Fieles a QGIS RESPALDO.py)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

SHEET_ID = '1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw'
CSV_URL = f'https://docs.google.com/spreadsheets/d/{SHEET_ID}/gviz/tq?tqx=out:csv&sheet=informe'

# --- MAPEOS COMPLETOS RESTAURADOS ---
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
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)':                  '_Caudal',
    'PRESION_(kg/cm2)':                '_Presion',
    'LONGITUD_DE_COLUMNA':             '_Long_colum',
    'COLUMNA_DIAMETRO_1':              '_Diam_colum',
    'TIPO_COLUMNA':                    '_Tipo_colum',
    'SECTOR_HIDRAULICO':               '_Sector',
    'NIVEL_DINAMICO_(mts)':            '_Nivel_Din',
    'NIVEL_ESTATICO_(mts)':            '_Nivel_Est',
    'EXTRACCION_MENSUAL_(m3)':         '_Vm_estr',
    'HORAS_DE_OPERACIÓN_DIARIA_(hrs)': '_Horas_op',
    'DISTRITO_1':                      '_Distrito',
    'ESTATUS':                         '_Estatus',
    'TELEMETRIA':                      '_Telemetria',
    'FECHA_ACTUALIZACION':             '_Ultima_actualizacion',
}

# --- 2. FUNCIONES DE LÓGICA ---

def limpiar_dato(v):
    if pd.isna(v) or v == "" or str(v).lower() == "nan": return None
    if isinstance(v, str):
        v = v.replace(',', '').strip()
        try: return float(v)
        except: return v
    return v

def ejecutar_sincronizacion_total():
    logs = [f"🚀 INICIO: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}"]
    try:
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        df['ID'] = df['ID'].astype(str).str.strip()

        # FASE SCADA: Solo valores > 0
        conn_s = mysql.connector.connect(**DB_SCADA)
        cur_s = conn_s.cursor(dictionary=True)
        hay_cambios = False
        
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                cur_s.execute("SELECT h.VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME = %s ORDER BY h.FECHA DESC LIMIT 1", (tag_name,))
                res = cur_s.fetchone()
                if res and res['VALUE'] is not None:
                    val_f = float(res['VALUE'])
                    if val_f > 0:
                        df.loc[df['POZOS'] == p_id, col_excel] = round(val_f, 2)
                        hay_cambios = True

        cur_s.close(); conn_s.close()

        # ACTUALIZAR GOOGLE SHEETS
        if hay_cambios:
            scope = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
            creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=scope)
            client = gspread.authorize(creds)
            sheet = client.open_by_key(SHEET_ID).worksheet("informe")
            sheet.update('A1', [df.fillna('').columns.values.tolist()] + df.fillna('').values.tolist())
            logs.append("✓ Google Sheets sincronizado.")

        # MYSQL INFORME
        engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{urllib.parse.quote_plus(DB_INFORME['password'])}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with engine_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            db_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
            df[df.columns.intersection(db_cols)].to_sql('INFORME', con=conn, if_exists='append', index=False)
        logs.append("✓ MySQL actualizado.")

        # POSTGRES (QGIS)
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        engine_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        with engine_pg.begin() as conn:
            for _, row in df.iterrows():
                id_m = str(row['ID']).strip()
                if not id_m or id_m == "nan": continue
                params = {"id": id_m}
                sets = []
                for c_csv, c_pg in MAPEO_POSTGRES.items():
                    if c_csv in df.columns:
                        params[c_pg] = limpiar_dato(row[c_csv])
                        sets.append(f'"{c_pg}" = :{c_pg}')
                if sets:
                    conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
        
        logs.append("✓ PostgreSQL (QGIS) actualizado.")
        return logs
    except Exception as e:
        return [f"❌ Error: {str(e)}"]

# --- 3. INTERFAZ (TIEMPO VACÍO + SEGUNDEROS) ---
st.title("🖥️ MIAA Data Center")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4, c5 = st.columns([1.5, 1, 1, 1.5, 1.5])
    
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"], index=None, placeholder="Elija modo...")
    with c2: h_in = st.number_input("Hora", 0, 23, value=None, placeholder="--")
    with c3: m_in = st.number_input("Min/Int", 0, 59, value=None, placeholder="--")
    
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR"
        bloqueado = h_in is None or m_in is None or modo is None
        if st.button(btn_label, use_container_width=True, disabled=bloqueado):
            st.session_state.running = not st.session_state.running
            st.rerun()
            
    with c5:
        if st.button("🚀 FORZAR CARGA", use_container_width=True):
            st.session_state.last_logs = ejecutar_sincronizacion_total()

# Consola Verde Original
log_txt = "<br>".join(st.session_state.get('last_logs', ["SISTEMA EN ESPERA..."]))
st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:Consolas;height:200px;overflow-y:auto;border-radius:5px;">{log_txt}</div>', unsafe_allow_html=True)

if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    if modo == "Diario":
        prox = ahora.replace(hour=h_in, minute=m_in, second=0, microsecond=0)
        if prox <= ahora: prox += datetime.timedelta(days=1)
    else:
        prox_m = ((ahora.minute // m_in) + 1) * m_in if m_in > 0 else 1
        prox = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=prox_m) if prox_m < 60 else ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
    
    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        st.session_state.last_logs = ejecutar_sincronizacion_total()
        st.rerun()
    
    time.sleep(1)
    st.rerun()
