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
st.set_page_config(page_title="MIAA FIX TOTAL - SCADA PRIORITY", layout="wide")

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
    'GASTO_(l.p.s.)': '_Caudal', 
    'PRESION_(kg/cm2)': '_Presion'
}

# --- 4. FUNCIONES ---

def obtener_scada_val(tags):
    try:
        conn = mysql.connector.connect(**DB_SCADA)
        cursor = conn.cursor(dictionary=True)
        cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({','.join(['%s']*len(tags))})", list(tags))
        t_map = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
        if not t_map: return {}
        cursor.execute(f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({','.join(['%s']*len(t_map))}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC", list(t_map.values()))
        res = {}
        for r in cursor.fetchall():
            if r['GATEID'] not in res: res[r['GATEID']] = r['VALUE']
        conn.close()
        return {name: res.get(gid) for name, gid in t_map.items()}
    except: return {}

def sync_agresiva():
    try:
        with st.status("🛠️ FORZANDO ACTUALIZACIÓN LIMPIA...", expanded=True) as status:
            # A. LEER Y LIMPIAR DATAFRAME (Elimina las columnas 'Unnamed')
            df = pd.read_csv(CSV_URL)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')] # <--- FIX PARA ERROR 1054
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # B. PRIORIDAD SCADA (Dato Maestro)
            tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
            scada_vals = obtener_scada_val(tags)

            for p_id, mapeo in MAPEO_SCADA.items():
                if p_id in df['ID'].values:
                    for col_ex, tag_sc in mapeo.items():
                        v = scada_vals.get(tag_sc)
                        if v is not None and float(v) > 0:
                            df.loc[df['ID'] == p_id, col_ex] = float(v)
                            st.write(f"🔥 Sobrescrito {p_id} con SCADA: {v}")

            # C. ESCRIBIR EN MYSQL (Solo columnas válidas)
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                # Obtenemos las columnas reales de la tabla para no mandar basura
                cols_reales = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df_final = df[df.columns.intersection(cols_reales)]
                df_final.to_sql('INFORME', con=conn, if_exists='append', index=False)

            # D. ESCRIBIR EN POSTGRES
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        params = {"pid": str(row['ID']).strip()}
                        updates = []
                        for c_ex, c_pg in MAPEO_POSTGRES.items():
                            if c_ex in df.columns:
                                updates.append(f'"{c_pg}" = :{c_pg}')
                                params[c_pg] = row[c_ex]
                        if updates:
                            conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(updates)} WHERE "ID" = :pid'), params)
            
            status.update(label="✅ ÉXITO TOTAL: SCADA mandó sobre Sheets", state="complete")
    except Exception as e:
        st.error(f"Error detectado: {e}")

# --- 5. INTERFAZ ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h = st.number_input("Hora", 0, 23, 13)
    with c3: m = st.number_input("Minuto", 0, 59, 1)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR"):
            st.session_state.running = not st.session_state.running
            st.rerun()

if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    # Lógica simplificada de segundero para no saturar
    st.write(f"🕒 Actual: {ahora.strftime('%H:%M:%S')}")
    if st.button("🚀 Sincronizar Manual Ahora"): sync_agresiva()
    time.sleep(1)
    st.rerun()
else:
    if st.button("🚀 Ejecutar Sincronización Manual"): sync_agresiva()
