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
st.set_page_config(page_title="MIAA Engine", layout="wide")

# Credenciales desde Secrets (Asegúrate de tenerlas configuradas en Streamlit)
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# Mapeos basados en tu respaldo
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS"
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal',
    'PRESION_(kg/cm2)': '_Presion',
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 2. LÓGICA DE DATOS ---

def limpiar_dato_para_postgres(valor):
    """Lógica de limpieza de tu respaldo"""
    if pd.isna(valor) or valor == "" or str(valor).lower() == "nan": return None
    if isinstance(valor, str):
        v = valor.replace(',', '').strip()
        try: return float(v)
        except: return valor
    return valor

def ejecutar_sincronizacion_silenciosa():
    try:
        # A. LEER SHEETS
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        df['ID'] = df['ID'].astype(str).str.strip()

        # B. FASE SCADA (Inyección Directa)
        conn_s = mysql.connector.connect(**DB_SCADA)
        cur_s = conn_s.cursor(dictionary=True)
        
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                # Buscamos el valor más reciente del tag
                query = """
                    SELECT h.VALUE FROM vfitagnumhistory h
                    JOIN VfiTagRef r ON h.GATEID = r.GATEID
                    WHERE r.NAME = %s AND h.FECHA >= NOW() - INTERVAL 1 HOUR
                    ORDER BY h.FECHA DESC LIMIT 1
                """
                cur_s.execute(query, (tag_name,))
                res = cur_s.fetchone()
                if res and res['VALUE'] is not None:
                    val_f = float(res['VALUE'])
                    if val_f > 0:
                        # Sobrescritura forzada en el DataFrame
                        df.loc[df['ID'] == p_id, col_excel] = val_f
        cur_s.close(); conn_s.close()

        # C. FASE MYSQL (INFORME)
        pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            # Filtro de columnas según tu respaldo
            cols_db = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
            df_my = df[df.columns.intersection(cols_db)].copy()
            df_my.to_sql('INFORME', con=conn, if_exists='append', index=False)

        # D. FASE POSTGRES (QGIS)
        pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        with eng_pg.connect() as conn:
            with conn.begin():
                for _, row in df.iterrows():
                    id_m = str(row['ID']).strip()
                    if not id_m or id_m == "nan": continue
                    
                    params = {"id": id_m}
                    sets = []
                    for c_excel, c_pg in MAPEO_POSTGRES.items():
                        if c_excel in df.columns:
                            # Usamos tu función de limpieza
                            params[c_pg] = limpiar_dato_para_postgres(row[c_excel])
                            sets.append(f'"{c_pg}" = :{c_pg}')
                    
                    if sets:
                        sql = f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'
                        conn.execute(text(sql), params)
        
        return True
    except Exception as e:
        st.error(f"Error en proceso: {e}")
        return False

# --- 3. INTERFAZ LIMPIA ---
st.title("MIAA Data System")

if "running" not in st.session_state: st.session_state.running = False

c1, c2 = st.columns(2)
with c1:
    if st.button("▶️ INICIAR MONITOR" if not st.session_state.running else "🛑 PARAR"):
        st.session_state.running = not st.session_state.running
        st.rerun()

with c2:
    if st.button("🚀 FORZAR CARGA MANUAL"):
        if ejecutar_sincronizacion_silenciosa():
            st.success("Sincronización completada.")

if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    st.write(f"Estado: Ejecutando... (Última revisión: {ahora.strftime('%H:%M:%S')})")
    
    # Ejecución cada 10 minutos (como en tu respaldo)
    if ahora.minute % 10 == 0 and ahora.second == 0:
        ejecutar_sincronizacion_silenciosa()
        time.sleep(1)
    
    time.sleep(1)
    st.rerun()
