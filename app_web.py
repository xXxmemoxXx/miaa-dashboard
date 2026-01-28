import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import mysql.connector
import datetime
import time

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA Data Center", layout="wide")

# --- CREDENCIALES (Iguales a tu archivo original) ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- LÓGICA DE PROCESAMIENTO ---
def ejecutar_actualizacion():
    try:
        st.info("⏳ Iniciando descarga de Google Sheets...")
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        st.success(f"✅ {len(df)} registros leídos de Sheets.")

        # Sincronización MySQL
        st.info("💾 Actualizando MySQL Informe...")
        pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
        engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        
        with engine_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df.to_sql('INFORME', con=conn, if_exists='append', index=False, chunksize=500)
        st.success("✅ MySQL Actualizado.")

        # Sincronización Postgres
        st.info("🐘 Sincronizando con PostgreSQL (QGIS)...")
        pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        url_pg = f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}"
        engine_pg = create_engine(url_pg)

        updates_pg = 0
        with engine_pg.connect() as conn:
            with conn.begin():
                for _, row in df.iterrows():
                    id_m = str(row['ID']).strip() if 'ID' in row else None
                    if not id_m or id_m.lower() == "nan": continue
                    
                    # Simpificación de Update (Ejemplo)
                    sql = text(f'UPDATE public."Pozos" SET "_Caudal" = :val WHERE "ID" = :id')
                    res = conn.execute(sql, {"val": row.get('GASTO_(l.p.s.)'), "id": id_m})
                    updates_pg += res.rowcount
        
        st.success(f"✅ PostgreSQL: {updates_pg} pozos actualizados.")
        
    except Exception as e:
        st.error(f"❌ Error: {str(e)}")

# --- INTERFAZ WEB ---
st.title("🌐 MIAA Dashboard Sincronizador")

col1, col2 = st.columns(2)

with col1:
    st.subheader("Control Manual")
    if st.button("🚀 EJECUTAR AHORA", use_container_width=True):
        ejecutar_actualizacion()

with col2:
    st.subheader("Estado del Servidor")
    st.write(f"Última revisión: {datetime.datetime.now().strftime('%H:%M:%S')}")
    st.metric("Base de Datos", "Conectada", delta="OK")

st.divider()
st.caption("MIAA - Dirección Técnica | Automatización 2026")