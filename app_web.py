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
st.set_page_config(page_title="MIAA Control Center - Full Automation", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEOS ---
MAPEO_SCADA = {
    "P-002": {"GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS", "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS"}
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'ESTATUS': '_Estatus', 
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. FUNCIONES DE DATOS ---

def obtener_valores_scada(tags):
    if not tags: return {}
    try:
        with mysql.connector.connect(**DB_SCADA) as conn:
            with conn.cursor(dictionary=True) as cursor:
                fmt = ','.join(['%s'] * len(tags))
                cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(tags))
                tag_to_id = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
                if not tag_to_id: return {}
                
                ids = list(tag_to_id.values())
                fmt_ids = ','.join(['%s'] * len(ids))
                query = f"SELECT GATEID, VALUE, FECHA FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC"
                cursor.execute(query, ids)
                
                id_to_data = {}
                for r in cursor.fetchall():
                    if r['GATEID'] not in id_to_data: id_to_data[r['GATEID']] = r
                return {name: id_to_data.get(gid) for name, gid in tag_to_id.items()}
    except Exception as e:
        return {}

def ejecutar_actualizacion():
    try:
        with st.status("🔄 Sincronizando (Lógica SCADA > 0)...", expanded=True) as status:
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            tags_scada = [t for p in MAPEO_SCADA.values() for t in p.values()]
            res_scada = obtener_valores_scada(tags_scada)
            
            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        data_s = res_scada.get(tag_scada)
                        if data_s and float(data_s['VALUE']) > 0:
                            df.loc[mask, col_excel] = data_s['VALUE']

            # Guardar MySQL e Inyectar en Postgres
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        params = {"id": str(row['ID']).strip()}
                        sets = [f'"{pg}" = :{pg}' for csv, pg in MAPEO_POSTGRES.items() if csv in df.columns]
                        for csv, pg in MAPEO_POSTGRES.items():
                            if csv in df.columns: params[pg] = None if pd.isna(row[csv]) else row[csv]
                        if sets: conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
            
            status.update(label="✅ Éxito", state="complete")
    except Exception as e:
        st.error(f"Error: {e}")

# --- 5. INTERFAZ ---
st.title("🖥️ MIAA Control Center")

# CONFIGURACIÓN DE TIEMPOS (Siempre visible)
with st.container(border=True):
    st.subheader("⚙️ Programación de Carga")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora", 0, 23, 8)
    with c3: m_in = st.number_input("Minuto / Intervalo", 0, 59, 0)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
            st.session_state.running = not st.session_state.running
            st.rerun()

tab_mon, tab_pg, tab_scada = st.tabs(["🚀 Monitor", "🐘 Base de Datos Postgres", "📈 Valor SCADA (P-002)"])

with tab_mon:
    if st.button("🔄 Sincronizar Manualmente"): ejecutar_actualizacion()
    if st.session_state.running:
        st.info("Monitor automático ejecutándose...")
        time.sleep(1); st.rerun()

with tab_pg:
    st.subheader("📋 Tabla Completa: public.\"Pozos\"")
    if st.button("🔍 Cargar Base de Datos Completa"):
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        df_full = pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', eng_pg)
        st.dataframe(df_full, use_container_width=True, height=500)

with tab_scada:
    st.subheader("📡 Monitoreo de Variable Específica")
    if st.button("🔦 Consultar PZ_002_TRC_CAU_INS"):
        val = obtener_valores_scada(["PZ_002_TRC_CAU_INS"])
        if "PZ_002_TRC_CAU_INS" in val:
            v = val["PZ_002_TRC_CAU_INS"]
            st.metric(label="Caudal P-002 (SCADA)", value=f"{v['VALUE']} lps", help=f"Última lectura: {v['FECHA']}")
            st.write(f"Si este valor es **> 0**, se usará en la base de datos. Si es **0**, se usará el valor de Sheets.")
        else:
            st.error("No se pudo obtener el valor del SCADA actualmente.")
