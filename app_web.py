import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import mysql.connector

# --- CONFIGURACIÓN DE SEGURIDAD (SECRETS) ---
# Esto lee las contraseñas desde el panel de Streamlit en lugar de tenerlas escritas aquí.
DB_SCADA = {
    'host': 'miaa.mx', 
    'user': 'miaamx_dashboard', 
    'password': st.secrets["db_scada"]["password"], 
    'database': 'miaamx_telemetria'
}
DB_INFORME = {
    'host': 'miaa.mx', 
    'user': 'miaamx_telemetria2', 
    'password': st.secrets["db_informe"]["password"], 
    'database': 'miaamx_telemetria2'
}
DB_POSTGRES = {
    'user': 'map_tecnica', 
    'pass': st.secrets["db_postgres"]["pass"], 
    'host': 'ti.miaa.mx', 
    'db': 'qgis', 
    'port': 5432
}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

st.title("📊 MIAA Data Center - Sincronizador Web")

if st.button("🚀 INICIAR SINCRONIZACIÓN"):
    try:
        with st.status("Procesando...", expanded=True) as status:
            st.write("📥 Descargando datos de Google Sheets...")
            df = pd.read_csv(CSV_URL)
            
            st.write("💾 Actualizando MySQL...")
            pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
            engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with engine_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                df.to_sql('INFORME', con=conn, if_exists='append', index=False)
            
            st.write("🐘 Sincronizando con PostgreSQL...")
            # Aquí va tu lógica de Postgres...
            
            status.update(label="✅ ¡Sincronización Completa!", state="complete")
        st.success("Bases de datos actualizadas correctamente.")
    except Exception as e:
        st.error(f"Error: {e}")