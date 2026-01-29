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
st.set_page_config(page_title="MIAA Control Maestro - Web Version", layout="wide")

# Credenciales (Fieles a tu archivo de respaldo)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeos extraídos de tu código
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS",
        "NIVEL_DINAMICO": "PZ_002_TRC_NIV_EST"
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal',
    'PRESION_(kg/cm2)': '_Presion',
    'ESTATUS': '_Estatus',
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 2. FUNCIONES DE LÓGICA (Tu código original) ---

def limpiar_dato_para_postgres(valor):
    if pd.isna(valor) or valor == "" or str(valor).lower() == "nan": return None
    if isinstance(valor, str):
        v = valor.replace(',', '').strip()
        try: return float(v)
        except: return valor
    return valor

def obtener_valores_scada():
    try:
        conn = mysql.connector.connect(**DB_SCADA)
        cursor = conn.cursor(dictionary=True)
        tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
        
        # Obtener GateIDs
        fmt = ','.join(['%s'] * len(tags))
        cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", tags)
        id_map = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
        
        if not id_map: return {}
        
        # Obtener Valores
        gids = list(id_map.values())
        fmt_ids = ','.join(['%s'] * len(gids))
        cursor.execute(f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 15 MINUTE ORDER BY FECHA DESC", gids)
        
        val_map = {}
        for r in cursor.fetchall():
            if r['GATEID'] not in val_map: val_map[r['GATEID']] = r['VALUE']
        
        conn.close()
        # Retornamos mapeado por NOMBRE de TAG para facilitar tu lógica de inyección
        return {name: val_map.get(gid) for name, gid in id_map.items()}
    except Exception as e:
        st.error(f"Error SCADA: {e}")
        return {}

def ejecutar_actualizacion_web():
    try:
        with st.status("🚀 Iniciando Proceso Maestro...", expanded=True) as status:
            # A. Leer Sheets
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()
            st.write(f"✅ Google Sheets leído: {len(df)} registros.")

            # B. Fase SCADA (Tu lógica de inyección prioritaria)
            scada_data = obtener_valores_scada()
            for p_id, config in MAPEO_SCADA.items():
                for col, tag in config.items():
                    val = scada_data.get(tag)
                    if val is not None:
                        try:
                            f_val = float(str(val).replace(',', ''))
                            if f_val != 0:
                                # Usamos 'ID' en lugar de 'POZOS' para mayor precisión en el cruce
                                df.loc[df['ID'] == p_id, col] = round(f_val, 2)
                                st.write(f"📡 SCADA -> {p_id} ({col}): {round(f_val, 2)}")
                        except: pass

            # C. Fase MySQL (Tabla INFORME)
            st.write("💾 Actualizando MySQL...")
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            engine_inf = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with engine_inf.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                res = conn.execute(text("SHOW COLUMNS FROM INFORME"))
                db_cols = [r[0] for r in res]
                # Esta línea es vital: solo mandamos lo que la DB acepta
                df_to_save = df[[c for c in df.columns if c in db_cols]].copy()
                df_to_save.to_sql('INFORME', con=conn, if_exists='append', index=False)

            # D. Fase Postgres (QGIS)
            st.write("🐘 Sincronizando Postgres...")
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            engine_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with engine_pg.begin() as conn:
                for _, row in df.iterrows():
                    id_m = str(row['ID']).strip()
                    if not id_m or id_m == "nan": continue
                    set_c = []; params = {"id": id_m}
                    for c_csv, c_pg in MAPEO_POSTGRES.items():
                        if c_csv in df.columns:
                            params[c_pg] = limpiar_dato_para_postgres(row[c_csv])
                            set_c.append(f'"{c_pg}" = :{c_pg}')
                    if set_c:
                        conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(set_c)} WHERE "ID" = :id'), params)
            
            status.update(label="✅ Sincronización Exitosa", state="complete")
    except Exception as e:
        st.error(f"❌ Error crítico: {e}")

# --- 3. INTERFAZ WEB ---
st.title("🖥️ MIAA Data Center - Web Sync")

with st.container(border=True):
    col1, col2, col3, col4 = st.columns(4)
    with col1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with col2: h_in = st.number_input("Hora (0-23)", 0, 23, 8)
    with col3: m_in = st.number_input("Min/Int", 1, 59, 10)
    with col4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR"):
            st.session_state.running = not st.session_state.running
            st.rerun()

if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    # Lógica de cálculo de tiempo igual a tu respaldo
    if modo == "Diario":
        prox = ahora.replace(hour=int(h_in), minute=int(m_in), second=0, microsecond=0)
        if prox <= ahora: prox += datetime.timedelta(days=1)
    else:
        prox_m = ((ahora.minute // int(m_in)) + 1) * int(m_in)
        if prox_m >= 60: prox = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        else: prox = ahora.replace(minute=prox_m, second=0, microsecond=0)
    
    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    if diff.total_seconds() <= 1:
        ejecutar_actualizacion_web()
        st.rerun()
    time.sleep(1)
    st.rerun()
else:
    if st.button("🚀 Sincronización Manual Ahora"):
        ejecutar_actualizacion_web()
