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
st.set_page_config(page_title="MIAA Control Maestro - FIX TOTAL", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEOS ---
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
    'NIVEL_DINAMICO': '_Nivel_Din',
    'ESTATUS': '_Estatus', 
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. MOTOR DE DATOS ---

def obtener_valor_scada(tags):
    try:
        conn = mysql.connector.connect(**DB_SCADA)
        cursor = conn.cursor(dictionary=True)
        fmt = ','.join(['%s'] * len(tags))
        cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(tags))
        t_map = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
        if not t_map: return {}
        
        ids = list(t_map.values())
        fmt_ids = ','.join(['%s'] * len(ids))
        cursor.execute(f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC", ids)
        
        vals = {}
        for r in cursor.fetchall():
            if r['GATEID'] not in vals: vals[r['GATEID']] = r['VALUE']
        conn.close()
        return {name: vals.get(gid) for name, gid in t_map.items()}
    except: return {}

def ejecutar_sincronizacion_maestra():
    try:
        with st.status("🔄 SINCRONIZANDO: PRIORIDAD SCADA", expanded=True) as status:
            # A. LEER SHEETS (Dato base)
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # B. OBTENER SCADA
            tags_scada = [t for p in MAPEO_SCADA.values() for t in p.values()]
            scada_vals = obtener_valor_scada(tags_scada)

            # C. FORZAR SOBRESCRITURA EN EL DATAFRAME (Dato Maestro)
            for p_id, mapeo in MAPEO_SCADA.items():
                idx = df[df['ID'] == p_id].index
                if not idx.empty:
                    for col_excel, tag_scada in mapeo.items():
                        v_scada = scada_vals.get(tag_scada)
                        if v_scada is not None and float(v_scada) > 0:
                            # SE USA .at PARA CAMBIO INMEDIATO EN MEMORIA
                            df.at[idx[0], col_excel] = float(v_scada)
                            st.write(f"✅ POZO {p_id}: Se grabó {v_scada} en memoria para {col_excel}")

            # D. ESCRIBIR EN MYSQL (TABLA INFORME)
            st.write("💾 Escribiendo en Tabla INFORME (MySQL)...")
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                # Filtrar solo columnas que existen en la tabla física
                cols_db = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df_mysql = df[df.columns.intersection(cols_db)].copy()
                df_mysql.to_sql('INFORME', con=conn, if_exists='append', index=False)

            # E. ESCRIBIR EN POSTGRES (COLUMNAS _Caudal, _Presion, etc.)
            st.write("🐘 Escribiendo en Postgres (QGIS)...")
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        p_id = str(row['ID']).strip()
                        updates = []
                        params = {"id": p_id}
                        for col_csv, col_pg in MAPEO_POSTGRES.items():
                            if col_csv in df.columns:
                                val = row[col_csv]
                                updates.append(f'"{col_pg}" = :{col_pg}')
                                params[col_pg] = None if pd.isna(val) else val
                        
                        if updates:
                            sql = f'UPDATE public."Pozos" SET {", ".join(updates)} WHERE "ID" = :id'
                            conn.execute(text(sql), params)
            
            status.update(label="✅ TERMINADO: Bases actualizadas con SCADA", state="complete")
    except Exception as e:
        st.error(f"Error Crítico: {e}")

# --- 5. INTERFAZ (RESTABLECIDA CON SEGUNDERO) ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    st.subheader("⚙️ Programación de Carga")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora", 0, 23, 13)
    with c3: m_in = st.number_input("Min/Int", 0, 59, 15)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR"):
            st.session_state.running = not st.session_state.running
            st.rerun()

if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    if modo == "Diario":
        prox = ahora.replace(hour=int(h_in), minute=int(m_in), second=0, microsecond=0)
        if prox <= ahora: prox += datetime.timedelta(days=1)
    else:
        int_m = int(m_in) if int(m_in) > 0 else 1
        prox_m = ((ahora.minute // int_m) + 1) * int_m
        if prox_m >= 60: prox = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        else: prox = ahora.replace(minute=prox_m, second=0, microsecond=0)
    
    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    st.write(f"🕒 Hora actual: **{ahora.strftime('%H:%M:%S')}**")
    
    if diff.total_seconds() <= 1:
        ejecutar_sincronizacion_maestra()
        st.rerun()
    time.sleep(1)
    st.rerun()

t_mon, t_pg, t_val = st.tabs(["🚀 Monitor", "🐘 Postgres", "📈 SCADA"])
with t_mon:
    if not st.session_state.running:
        if st.button("🚀 Ejecutar Manual"): ejecutar_sincronizacion_maestra()
with t_pg:
    if st.button("🔍 Ver Tabla Pozos"):
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        st.dataframe(pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID"', eng_pg))
with t_val:
    if st.button("Consultar P-002"):
        v = obtener_valor_scada(["PZ_002_TRC_CAU_INS"])
        st.metric("Caudal SCADA", f"{v.get('PZ_002_TRC_CAU_INS', 0)} lps")
