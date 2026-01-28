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

# --- 3. MAPEO SCADA ---
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS"
    }
}

# --- 4. FUNCIONES DE DATOS ---

def obtener_datos_scada_reales(tags):
    """Consulta directa al SCADA para obtener valores actuales."""
    try:
        conn = mysql.connector.connect(**DB_SCADA)
        cursor = conn.cursor(dictionary=True)
        # Obtener IDs de los nombres
        fmt = ','.join(['%s'] * len(tags))
        cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(tags))
        tag_map = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
        
        if not tag_map: return {}
        
        # Obtener últimos valores
        ids = list(tag_map.values())
        fmt_ids = ','.join(['%s'] * len(ids))
        cursor.execute(f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC", ids)
        
        vals = {}
        for r in cursor.fetchall():
            if r['GATEID'] not in vals: vals[r['GATEID']] = r['VALUE']
        
        conn.close()
        return {name: vals.get(gid) for name, gid in tag_map.items()}
    except: return {}

def ejecutar_actualizacion():
    try:
        with st.status("🔄 Sincronizando: Prioridad SCADA > 0...", expanded=True) as status:
            # A. Leer Excel
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # B. LÓGICA DE SOBRESCRITURA (Aquí estaba el fallo)
            st.write("📡 Validando valores de SCADA...")
            tags_necesarios = [t for p in MAPEO_SCADA.values() for t in p.values()]
            scada_vals = obtener_datos_scada_reales(tags_necesarios)

            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        val_s = scada_vals.get(tag_scada)
                        # SI EL DATO EXISTE Y ES MAYOR A CERO, SE ESCRIBE EN EL DATAFRAME
                        if val_s is not None and float(val_s) > 0:
                            df.loc[mask, col_excel] = val_s
                            st.write(f"✅ {p_id}: Usando SCADA ({val_s}) para {col_excel}")

            # C. ESCRIBIR EN TABLA INFORME (MySQL)
            st.write("💾 Actualizando Tabla INFORME...")
            pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                res = conn.execute(text("SHOW COLUMNS FROM INFORME"))
                cols_db = [r[0] for r in res]
                df[df.columns.intersection(cols_db)].to_sql('INFORME', con=conn, if_exists='append', index=False)

            # D. ESCRIBIR EN POSTGRES (QGIS)
            st.write("🐘 Actualizando Postgres...")
            pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            # ... (Aquí va tu bloque de UPDATE en Postgres que ya tienes)
            
            status.update(label="✅ Sincronización Exitosa en todas las bases", state="complete")
    except Exception as e:
        st.error(f"Error: {e}")

# --- 5. INTERFAZ (Con tus pestañas solicitadas) ---
st.title("🖥️ MIAA Control Center")

# Panel de tiempos (Igual al tuyo)
with st.container(border=True):
    st.subheader("⚙️ Configuración de Tiempo")
    # ... (Tus inputs de hora/minuto)

tab_mon, tab_pg, tab_scada = st.tabs(["🚀 Monitor", "🐘 Base de Datos Postgres", "📈 Valor SCADA"])

with tab_mon:
    if st.button("🚀 Ejecutar Sincronización Manual"):
        ejecutar_actualizacion()

with tab_pg:
    st.subheader("Vista Completa de Postgres")
    if st.button("🔍 Ver Tabla Completa"):
        pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        st.dataframe(pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID"', eng_pg), use_container_width=True)

with tab_scada:
    st.subheader("Monitoreo Variable Específica")
    if st.button("Consultar PZ_002_TRC_CAU_INS"):
        val = obtener_datos_scada_reales(["PZ_002_TRC_CAU_INS"])
        st.metric("Caudal Actual P002", f"{val.get('PZ_002_TRC_CAU_INS', 0)} lps")
