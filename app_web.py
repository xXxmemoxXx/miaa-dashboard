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
st.set_page_config(page_title="MIAA FIX - PRIORIDAD SCADA", layout="wide")

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
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'NIVEL_DINAMICO': '_Nivel_Din',
    'ESTATUS': '_Estatus', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. FUNCIONES DE DATOS ---

def obtener_scada_directo(tags):
    try:
        conn = mysql.connector.connect(**DB_SCADA)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({','.join(['%s']*len(tags))})", list(tags))
        t_map = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
        if not t_map: return {}
        
        cursor.execute(f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({','.join(['%s']*len(t_map))}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC", list(t_map.values()))
        vals = {}
        for r in cursor.fetchall():
            if r['GATEID'] not in vals: vals[r['GATEID']] = r['VALUE']
        conn.close()
        return {name: vals.get(gid) for name, gid in t_map.items()}
    except: return {}

def proceso_sincronizacion_real():
    try:
        with st.status("🛠️ FORZANDO ACTUALIZACIÓN...", expanded=True) as status:
            # 1. Carga inicial
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # 2. Obtener SCADA
            tags_necesarios = [t for p in MAPEO_SCADA.values() for t in p.values()]
            datos_scada = obtener_scada_directo(tags_necesarios)

            # 3. ELIMINAR Y REEMPLAZAR (Fuerza bruta sobre el DataFrame)
            for p_id, mapeo in MAPEO_SCADA.items():
                if p_id in df['ID'].values:
                    for col_excel, tag_scada in mapeo.items():
                        v_real = datos_scada.get(tag_scada)
                        if v_real is not None and float(v_real) > 0:
                            # Esto asegura que el valor de Sheets (18.8) MUERA en la memoria
                            df.loc[df['ID'] == p_id, col_excel] = float(v_real)
                            st.write(f"🔥 PRIORIDAD: {p_id} -> {col_excel} = {v_real} (SCADA)")

            # 4. MySQL (INFORME)
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                df.to_sql('INFORME', con=conn, if_exists='append', index=False)

            # 5. POSTGRES (QGIS) - AQUÍ ESTABA EL FALLO
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        # Creamos los parámetros DIRECTAMENTE del row procesado
                        params = {"pid": str(row['ID']).strip()}
                        updates = []
                        for c_excel, c_pg in MAPEO_POSTGRES.items():
                            if c_excel in df.columns:
                                updates.append(f'"{c_pg}" = :{c_pg}')
                                params[c_pg] = row[c_excel] if not pd.isna(row[c_excel]) else None
                        
                        if updates:
                            # Ejecución atómica por ID
                            q = text(f'UPDATE public."Pozos" SET {", ".join(updates)} WHERE "ID" = :pid')
                            conn.execute(q, params)
            
            status.update(label="✅ SINCRONIZACIÓN TOTAL FINALIZADA", state="complete")
    except Exception as e:
        st.error(f"Error: {e}")

# --- 5. INTERFAZ Y SEGUNDERO ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    st.subheader("⚙️ Programación")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora", 0, 23, 13)
    with c3: m_in = st.number_input("Min/Int", 0, 59, 1)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
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
    st.metric("⏳ SIGUIENTE CARGA EN:", str(diff).split('.')[0])
    st.write(f"🕒 Hora actual: **{ahora.strftime('%H:%M:%S')}**")
    
    if diff.total_seconds() <= 1:
        proceso_sincronizacion_real()
        st.rerun()
    time.sleep(1)
    st.rerun()
else:
    if st.button("🚀 Ejecutar Ahora (Manual)"): proceso_sincronizacion_real()
