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
st.set_page_config(page_title="MIAA Control Center - Full Fix", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEO SCADA (Corregido para P002) ---
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS"
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal',
    'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum',
    'NIVEL_DINAMICO_(mts)': '_Nivel_Din',
    'ESTATUS': '_Estatus',
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. FUNCIONES DE DATOS ---

def consultar_scada_real(tags):
    """Obtiene los valores actuales del SCADA para sobrescribir el Excel."""
    try:
        with mysql.connector.connect(**DB_SCADA) as conn:
            with conn.cursor(dictionary=True) as cursor:
                # 1. Obtener IDs
                fmt = ','.join(['%s'] * len(tags))
                cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(tags))
                tag_to_id = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
                if not tag_to_id: return {}

                # 2. Obtener Valores
                ids = list(tag_to_id.values())
                fmt_ids = ','.join(['%s'] * len(ids))
                query = f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC"
                cursor.execute(query, ids)
                
                id_to_val = {}
                for r in cursor.fetchall():
                    if r['GATEID'] not in id_to_val: id_to_val[r['GATEID']] = r['VALUE']
                
                return {name: id_to_val.get(gid) for name, gid in tag_to_id.items()}
    except: return {}

def ejecutar_actualizacion():
    try:
        with st.status("🔄 Sincronizando: Prioridad SCADA > 0...", expanded=True) as status:
            # A. Leer Excel (Respaldo)
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # B. Consultar SCADA y SOBREESCRIBIR si el valor es > 0
            st.write("📡 Consultando SCADA para validación...")
            tags_a_consultar = [t for p in MAPEO_SCADA.values() for t in p.values()]
            valores_actuales = consultar_scada_real(tags_a_consultar)

            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        val_scada = valores_actuales.get(tag_scada)
                        # REGLA: Si SCADA es > 0, borramos el dato del Excel y ponemos el del SCADA
                        if val_scada is not None and float(val_scada) > 0:
                            df.loc[mask, col_excel] = val_scada
                            st.write(f"✅ {p_id}: Usando SCADA {val_scada} (Sobrescribió al Excel)")

            # C. Guardar en Postgres (QGIS)
            pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            engine_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with engine_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        id_val = str(row['ID']).strip()
                        params = {"id": id_val}
                        sets = []
                        for csv_col, pg_col in MAPEO_POSTGRES.items():
                            if csv_col in df.columns:
                                val = row[csv_col]
                                sets.append(f'"{pg_col}" = :{pg_col}')
                                params[pg_col] = None if pd.isna(val) else val
                        if sets:
                            conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
            
            status.update(label="✅ Sincronización Completa", state="complete")
    except Exception as e:
        st.error(f"Error: {e}")

# --- 5. INTERFAZ ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    st.subheader("⚙️ Configuración de Tiempo")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: hora_in = st.number_input("Hora (0-23)", 0, 23, 12)
    with c3: min_in = st.number_input("Minuto / Intervalo", 0, 59, 0)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
            st.session_state.running = not st.session_state.running
            st.rerun()

tab_mon, tab_pg, tab_scada = st.tabs(["🚀 Monitor", "🐘 Base de Datos Postgres", "📈 Valor SCADA"])

with tab_mon:
    if st.button("🚀 Forzar Sincronización"): ejecutar_actualizacion()
    if st.session_state.running:
        st.write(f"🕒 Hora local: **{datetime.datetime.now(zona_local).strftime('%H:%M:%S')}**")
        time.sleep(1); st.rerun()

with tab_pg:
    st.subheader("📋 Tabla Completa public.\"Pozos\"")
    if st.button("🔍 Ver Base de Datos Completa"):
        pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        engine_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        df_full = pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', engine_pg)
        st.dataframe(df_full, use_container_width=True)

with tab_scada:
    st.subheader("📡 Monitoreo de Variable")
    if st.button("Consultar PZ_002_TRC_CAU_INS"):
        val = consultar_scada_real(["PZ_002_TRC_CAU_INS"])
        st.metric("Caudal P002", f"{val.get('PZ_002_TRC_CAU_INS', 'N/A')} lps")
