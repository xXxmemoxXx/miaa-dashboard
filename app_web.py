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
st.set_page_config(page_title="MIAA Sync Engine - SCADA FIX", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEOS (P-002 ESPECÍFICO) ---
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

# --- 4. FUNCIONES ---

def consultar_scada_real(tags):
    try:
        conn = mysql.connector.connect(**DB_SCADA)
        cursor = conn.cursor(dictionary=True)
        # Usamos VALUE en lugar de VAL para evitar el error 1054 que sale en tu captura
        fmt = ','.join(['%s'] * len(tags))
        query = f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})"
        cursor.execute(query, list(tags))
        t_map = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
        
        if not t_map: return {}
        
        ids = list(t_map.values())
        fmt_ids = ','.join(['%s'] * len(ids))
        cursor.execute(f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC", ids)
        
        res = {}
        for r in cursor.fetchall():
            if r['GATEID'] not in res: res[r['GATEID']] = r['VALUE']
        conn.close()
        return {name: res.get(gid) for name, gid in t_map.items()}
    except Exception as e:
        st.error(f"Error SCADA: {e}")
        return {}

def sincronizar_todo():
    try:
        with st.status("🚀 Iniciando Sincronización Masiva...", expanded=True) as status:
            # A. Leer Sheets (Dato base 18.8)
            st.write("📖 Leyendo Google Sheets...")
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # B. Actualizar datos desde SCADA (Dato real 19.124)
            st.write("🔍 Actualizando datos desde SCADA...")
            tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
            scada_vals = consultar_scada_real(tags)

            for p_id, mapeo in MAPEO_SCADA.items():
                for col_excel, tag_scada in mapeo.items():
                    val_s = scada_vals.get(tag_scada)
                    if val_s is not None and float(val_s) > 0:
                        # REEMPLAZO FORZADO EN EL DATAFRAME
                        df.loc[df['ID'] == p_id, col_excel] = float(val_s)
                        st.success(f"POZO {p_id}: Se asignó {val_s} de SCADA a la columna {col_excel}")

            # C. Escribir en MySQL (Tabla INFORME)
            st.write("💾 Guardando en MySQL (Tabla INFORME)...")
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                cols_db = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df[df.columns.intersection(cols_db)].to_sql('INFORME', con=conn, if_exists='append', index=False)

            # D. Escribir en Postgres (QGIS)
            st.write("🐘 Sincronizando con Postgres (QGIS)...")
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
                                # Aquí nos aseguramos que el valor que viaja es el del DataFrame ya corregido
                                val_final = row[c_csv]
                                params[c_pg] = None if pd.isna(val_final) else val_final
                        if sets:
                            conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
            
            status.update(label="✅ Sincronización Exitosa", state="complete")
    except Exception as e:
        st.error(f"Error General: {e}")

# --- 5. INTERFAZ (RESTABLECIDA) ---
st.title("🖥️ MIAA Data Center - Sync Engine")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora (0-23)", 0, 23, 13)
    with c3: m_in = st.number_input("Minuto / Intervalo", 0, 59, 15)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR MONITOR"
        if st.button(btn_label, use_container_width=True, type="primary" if st.session_state.running else "secondary"):
            st.session_state.running = not st.session_state.running
            st.rerun()

# Lógica del Segundero
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
    st.write(f"🕒 Hora local: **{ahora.strftime('%H:%M:%S')}**")
    st.metric("⏳ SIGUIENTE CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        sincronizar_todo()
        st.rerun()
    time.sleep(1)
    st.rerun()

# Pestañas de visualización
t_mon, t_db, t_scada = st.tabs(["🚀 Monitor", "🐘 Base de Datos Postgres", "📈 Valor SCADA (P-002)"])

with t_mon:
    if not st.session_state.running:
        st.info("Estatus: Detenido. Ajuste el tiempo e inicie.")
        if st.button("🚀 Forzar Sincronización Manual"): sincronizar_todo()

with t_db:
    if st.button("🔍 Cargar Datos de Postgres"):
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        st.dataframe(pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID"', eng_pg), use_container_width=True)

with t_scada:
    if st.button("Consultar PZ_002_TRC_CAU_INS"):
        val = consultar_scada_real(["PZ_002_TRC_CAU_INS"])
        st.metric("Caudal P-002 (SCADA)", f"{val.get('PZ_002_TRC_CAU_INS', 0)} lps")
