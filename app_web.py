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
st.set_page_config(page_title="MIAA Control Center - SCADA Priority", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEO SCADA (Variables críticas) ---
MAPEO_SCADA = {
    "P-002": {"GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS", "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS"},
    "P-003": {"GASTO_(l.p.s.)": "PZ_003_CAU_INS", "PRESION_(kg/cm2)": "PZ_003_PRES_INS"},
    "P-004": {"GASTO_(l.p.s.)": "PZ_004_CAU_INS", "PRESION_(kg/cm2)": "PZ_004_PRES_INS"}
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'NIVEL_DINAMICO_(mts)': '_Nivel_Din',
    'ESTATUS': '_Estatus', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. FUNCIONES DE APOYO ---

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
                query = f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC"
                cursor.execute(query, ids)
                
                id_to_val = {}
                for r in cursor.fetchall():
                    if r['GATEID'] not in id_to_val: id_to_val[r['GATEID']] = r['VALUE']
                return {name: id_to_val.get(gid) for name, gid in tag_to_id.items()}
    except Exception as e:
        st.error(f"Error SCADA: {e}")
        return {}

def ejecutar_actualizacion():
    try:
        with st.status("🚀 Iniciando Sincronización Real...", expanded=True) as status:
            # A. Leer Excel
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # B. Consultar SCADA y SOBREESCRIBIR el DataFrame
            st.write("🔍 Extrayendo datos frescos de SCADA...")
            todos_tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
            vals_scada = obtener_valores_scada(todos_tags)
            
            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        if tag_scada in vals_scada:
                            val_real = vals_scada[tag_scada]
                            df.loc[mask, col_excel] = val_real
                            st.write(f"✅ {p_id}: Actualizado {col_excel} a {val_real}")

            # C. Guardar en MySQL Informe (Dato ya corregido)
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                db_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df[df.columns.intersection(db_cols)].to_sql('INFORME', con=conn, if_exists='append', index=False)

            # D. Guardar en Postgres (Dato ya corregido)
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        params = {"id": str(row['ID']).strip()}
                        sets = []
                        for c_csv, c_pg in MAPEO_POSTGRES.items():
                            if c_csv in df.columns:
                                val = row[c_csv]
                                sets.append(f'"{c_pg}" = :{c_pg}')
                                params[c_pg] = None if pd.isna(val) else val
                        if sets:
                            conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)

            status.update(label="✅ Sincronización Exitosa", state="complete")
    except Exception as e:
        st.error(f"Fallo en la sincronización: {e}")

# --- 5. INTERFAZ ---
tab_mon, tab_pg = st.tabs(["🚀 Monitor", "🔍 Postgres"])

with tab_mon:
    st.header("MIAA Sync Engine")
    
    # Reparación del error de NameError definiendo valores por defecto
    if "running" not in st.session_state: st.session_state.running = False
    
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h = st.number_input("Hora", 0, 23, 12)
    with c3: m = st.number_input("Min/Int", 0, 59, 0)
    with c4:
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
            st.session_state.running = not st.session_state.running
            st.rerun()

    if st.button("🚀 Forzar Sincronización Manual (SCADA Priority)", type="primary"):
        ejecutar_actualizacion()

    if st.session_state.running:
        ahora = datetime.datetime.now(zona_local)
        st.write(f"🕒 Hora actual: **{ahora.strftime('%H:%M:%S')}**")
        time.sleep(1)
        st.rerun()

with tab_pg:
    if st.button("Verificar datos en Postgres"):
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        df_pg = pd.read_sql('SELECT "ID", "_Caudal", "_Presion" FROM public."Pozos" WHERE "ID" = \'P002\'', eng_pg)
        st.table(df_pg)
