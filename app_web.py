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
st.set_page_config(page_title="MIAA Control Maestro - SCADA Priority", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEOS (RESTAURADOS Y COMPLETOS) ---
MAPEO_SCADA = {
    "P-002": {"GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS", "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS", "NIVEL_DINAMICO": "PZ_002_TRC_NIV_EST"},
    "P-003": {"GASTO_(l.p.s.)": "PZ_003_CAU_INS", "PRESION_(kg/cm2)": "PZ_003_PRES_INS", "NIVEL_DINAMICO": "PZ_003_NIV_EST"},
    "P-004": {"GASTO_(l.p.s.)": "PZ_004_CAU_INS", "PRESION_(kg/cm2)": "PZ_004_PRES_INS", "NIVEL_DINAMICO": "PZ_004_NIV_EST"}
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 
    'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 
    'NIVEL_DINAMICO': '_Nivel_Din',
    'ESTATUS': '_Estatus', 
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. FUNCIONES ---

def obtener_scada_fresco(tags):
    try:
        with mysql.connector.connect(**DB_SCADA) as conn:
            with conn.cursor(dictionary=True) as cursor:
                fmt = ','.join(['%s'] * len(tags))
                cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(tags))
                t_map = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
                if not t_map: return {}
                ids = list(t_map.values())
                fmt_ids = ','.join(['%s'] * len(ids))
                cursor.execute(f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC", ids)
                res = {}
                for r in cursor.fetchall():
                    if r['GATEID'] not in res: res[r['GATEID']] = r['VALUE']
                return {name: res.get(gid) for name, gid in t_map.items()}
    except: return {}

def proceso_sincronizacion_total():
    try:
        with st.status("🚀 PRIORIDAD SCADA: Sobrescribiendo Google Sheets...", expanded=True) as status:
            # A. Leer Sheets (Como base técnica)
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # B. Consultar SCADA
            tags_a_buscar = [t for p in MAPEO_SCADA.values() for t in p.values()]
            valores_reales = obtener_scada_fresco(tags_a_buscar)

            # C. EL REEMPLAZO AGRESIVO (Aquí es donde el 19.11 toma el control)
            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        val_scada = valores_reales.get(tag_scada)
                        # SI SCADA TIENE DATO > 0, SE IMPONE SOBRE EL SHEETS
                        if val_scada is not None and float(val_scada) > 0:
                            df.loc[mask, col_excel] = val_scada
                            st.write(f"✅ {p_id}: Se impuso SCADA ({val_scada}) sobre el valor de Sheets.")

            # D. Actualizar MySQL INFORME (Con el dato corregido)
            st.write("💾 Actualizando Tabla INFORME...")
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                cols_db = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df[df.columns.intersection(cols_db)].to_sql('INFORME', con=conn, if_exists='append', index=False)

            # E. Actualizar POSTGRES QGIS (Con el dato corregido)
            st.write("🐘 Actualizando Postgres...")
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        params = {"id": str(row['ID']).strip()}
                        sets = []
                        for c_csv, c_pg in MAPEO_POSTGRES.items():
                            if c_csv in df.columns:
                                sets.append(f'"{c_pg}" = :{c_pg}')
                                params[c_pg] = None if pd.isna(row[c_csv]) else row[c_csv]
                        if sets: conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
            
            status.update(label="✅ ÉXITO: SCADA priorizado correctamente", state="complete")
    except Exception as e: st.error(f"Error: {e}")

# --- 5. INTERFAZ (EL SEGUNDERO Y LOS CUADROS) ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    st.subheader("⚙️ Programación de Tiempo")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora (0-23)", 0, 23, 8)
    with c3: m_in = st.number_input("Minuto / Intervalo", 0, 59, 1)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
            st.session_state.running = not st.session_state.running
            st.rerun()

tab_mon, tab_pg, tab_scada = st.tabs(["🚀 Monitor & Segundero", "🐘 Postgres Completo", "📈 Valor SCADA"])

with tab_mon:
    if st.session_state.running:
        ahora = datetime.datetime.now(zona_local)
        if modo == "Diario":
            proximo = ahora.replace(hour=int(h_in), minute=int(m_in), second=0, microsecond=0)
            if proximo <= ahora: proximo += datetime.timedelta(days=1)
        else:
            int_m = int(m_in) if int(m_in) > 0 else 1
            prox_m = ((ahora.minute // int_m) + 1) * int_m
            if prox_m >= 60: proximo = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
            else: proximo = ahora.replace(minute=prox_m, second=0, microsecond=0)
        
        diff = proximo - ahora
        st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
        st.write(f"🕒 Hora actual: **{ahora.strftime('%H:%M:%S')}**")
        
        if diff.total_seconds() <= 1:
            proceso_sincronizacion_total()
            st.rerun()
        time.sleep(1)
        st.rerun()
    else:
        st.info("Monitor en pausa.")
        if st.button("🚀 Forzar Sincronización Manual"): proceso_sincronizacion_total()

with tab_pg:
    if st.button("🔍 Cargar Tabla Completa"):
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        st.dataframe(pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID"', eng_pg), use_container_width=True)

with tab_scada:
    if st.button("Consultar P-002"):
        val = obtener_scada_fresco(["PZ_002_TRC_CAU_INS"])
        st.metric("Caudal Directo SCADA (P002)", f"{val.get('PZ_002_TRC_CAU_INS', 0)} lps")
