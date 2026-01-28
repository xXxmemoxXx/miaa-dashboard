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
    "P-002": {"GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS", "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS", "NIVEL_DINAMICO": "PZ_002_TRC_NIV_EST"},
    "P-003": {"GASTO_(l.p.s.)": "PZ_003_CAU_INS", "PRESION_(kg/cm2)": "PZ_003_PRES_INS", "NIVEL_DINAMICO": "PZ_003_NIV_EST"},
    "P-004": {"GASTO_(l.p.s.)": "PZ_004_CAU_INS", "PRESION_(kg/cm2)": "PZ_004_PRES_INS", "NIVEL_DINAMICO": "PZ_004_NIV_EST"},
    "P-005A": {"GASTO_(l.p.s.)": "PZ_RP_005_TRHDAS_CAU_INS", "PRESION_(kg/cm2)": "PZ_RP_005_TRHDAS_PRES_INS", "NIVEL_DINAMICO": "PZ_RP_005_TRHDAS_NIV_EST"}
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'NIVEL_DINAMICO_(mts)': '_Nivel_Din',
    'ESTATUS': '_Estatus', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
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
                query = f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC"
                cursor.execute(query, ids)
                
                id_to_val = {}
                for r in cursor.fetchall():
                    if r['GATEID'] not in id_to_val: id_to_val[r['GATEID']] = r['VALUE']
                return {name: id_to_val.get(gid) for name, gid in tag_to_id.items()}
    except Exception: return {}

def ejecutar_actualizacion():
    try:
        with st.status("🔄 Iniciando Sincronización Inteligente...", expanded=True) as status:
            # 1. Leer Google Sheets (Base Respaldo)
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # 2. Consultar SCADA
            todos_tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
            vals_scada = obtener_valores_scada(todos_tags)
            
            # 3. Lógica Prioridad: SCADA > 0 ? SCADA : Sheets
            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        val_scada = vals_scada.get(tag_scada)
                        # SOLO sobreescribe si SCADA es mayor a cero
                        if val_scada is not None and float(val_scada) > 0:
                            df.loc[mask, col_excel] = val_scada

            # 4. Actualizar MySQL Informe
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                db_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df[df.columns.intersection(db_cols)].to_sql('INFORME', con=conn, if_exists='append', index=False)

            # 5. Actualizar Postgres QGIS
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
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

            status.update(label="✅ Sincronización Exitosa", state="complete")
            st.toast("Datos actualizados correctamente")
    except Exception as e:
        st.error(f"Error Crítico: {e}")

# --- 5. INTERFAZ ---
st.title("🖥️ MIAA Control Center")

# --- CONFIGURACIÓN DE TIEMPOS ---
with st.container(border=True):
    st.subheader("⚙️ Configuración de Ejecución")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: h_in = st.number_input("Hora (0-23)", 0, 23, 8)
    with c3: m_in = st.number_input("Minuto / Intervalo", 0, 59, 0)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_text = "🛑 DETENER" if st.session_state.running else "▶️ INICIAR"
        if st.button(btn_text, use_container_width=True, type="primary" if st.session_state.running else "secondary"):
            st.session_state.running = not st.session_state.running
            st.rerun()

# --- PESTAÑAS ---
tab_mon, tab_pg = st.tabs(["🚀 Monitor", "🔍 Base de Datos Postgres (Completa)"])

with tab_mon:
    if st.button("🔄 Sincronización Manual (SCADA > 0)"):
        ejecutar_actualizacion()
        
    if st.session_state.running:
        ahora = datetime.datetime.now(zona_local)
        # Lógica de cálculo simplificada para visualización
        st.write(f"🕒 Monitor activo. Hora local: **{ahora.strftime('%H:%M:%S')}**")
        # Aquí se ejecutaría el trigger de tiempo automático según tu lógica previa
        time.sleep(1)
        st.rerun()
    else:
        st.info("El monitor automático está en pausa.")

with tab_pg:
    st.subheader("📋 Tabla Completa: public.\"Pozos\"")
    if st.button("🔄 Cargar/Refrescar Base Completa"):
        try:
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            # Traemos TODAS las columnas (*)
            df_full = pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', eng_pg)
            
            st.write(f"Mostrando {len(df_full)} registros encontrados en Postgres.")
            st.dataframe(df_full, use_container_width=True, height=600)
            
        except Exception as e:
            st.error(f"Error al conectar con Postgres: {e}")
