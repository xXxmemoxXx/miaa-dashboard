import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA Control Center", layout="wide")

# --- 1. CONFIGURACIÓN DE CONEXIONES (Usando Secrets) ---
# Extraído de la configuración de QGIS RESPALDO.py
DB_SCADA = {
    'host': 'miaa.mx', 'user': 'miaamx_dashboard', 
    'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'
}
DB_INFORME = {
    'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 
    'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'
}
DB_POSTGRES = {
    'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 
    'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432
}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 2. MAPEOS (Originales de tu respaldo) ---
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
        "VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2",
        "VOLTAJE_L2":"PZ_002_TRC_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_002_TRC_VOL_L1_L3",
        "AMP_L1":"PZ_002_TRC_CORR_L1",
        "AMP_L2":"PZ_002_TRC_CORR_L2",
        "AMP_L3":"PZ_002_TRC_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM",
        "SUMERGENCIA":"PZ_002_TRC_SUMERG",
        "NIVEL_DINAMICO":"PZ_002_TRC_NIV_EST",
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)':                  '_Caudal',
    'PRESION_(kg/cm2)':                '_Presion',
    'LONGITUD_DE_COLUMNA':             '_Long_colum',
    'COLUMNA_DIAMETRO_1':              '_Diam_colum',
    'TIPO_COLUMNA':                    '_Tipo_colum',
    'SECTOR_HIDRAULICO':               '_Sector',
    'NIVEL_DINAMICO_(mts)':            '_Nivel_Din',
    'NIVEL_ESTATICO_(mts)':            '_Nivel_Est',
    'EXTRACCION_MENSUAL_(m3)':         '_Vm_estr',
    'HORAS_DE_OPERACIÓN_DIARIA_(hrs)': '_Horas_op',
    'DISTRITO_1':                      '_Distrito',
    'ESTATUS':                         '_Estatus',
    'TELEMETRIA':                      '_Telemetria',
    'FECHA_ACTUALIZACION':             '_Ultima_actualizacion',
}

# --- 3. FUNCIONES DE LÓGICA ---
def ejecutar_actualizacion():
    try:
        with st.status("🔄 Sincronizando bases de datos...", expanded=True) as status:
            # Lectura de Google Sheets
            st.write("📥 Leyendo Google Sheets...")
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]

            # Sincronización MySQL Informe
            st.write("💾 Actualizando MySQL...")
            pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
            engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            
            with engine_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                res_cols = conn.execute(text("SHOW COLUMNS FROM INFORME"))
                db_cols = [r[0] for r in res_cols]
                df_to_save = df[[c for c in df.columns if c in db_cols]].copy()
                df_to_save.to_sql('INFORME', con=conn, if_exists='append', index=False)
            
            # Sincronización PostgreSQL (QGIS)
            st.write("🐘 Actualizando PostgreSQL...")
            pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            url_pg = f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}"
            engine_pg = create_engine(url_pg)

            updates_pg = 0
            with engine_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        id_m = str(row['ID']).strip() if 'ID' in row else None
                        if not id_m or id_m.lower() == "nan": continue
                        
                        set_clauses = []
                        params = {"id": id_m}
                        for col_csv, col_pg in MAPEO_POSTGRES.items():
                            if col_csv in df.columns:
                                val = row[col_csv]
                                if pd.isna(val) or val == "": val = None
                                set_clauses.append(f'"{col_pg}" = :{col_pg}')
                                params[col_pg] = val
                        
                        if set_clauses:
                            sql = text(f'UPDATE public."Pozos" SET {", ".join(set_clauses)} WHERE "ID" = :id')
                            res = conn.execute(sql, params)
                            updates_pg += res.rowcount
            
            status.update(label="✅ ¡Sincronización Exitosa!", state="complete")
            st.toast(f"MySQL actualizado y {updates_pg} pozos en QGIS.", icon="🚀")
            return True
    except Exception as e:
        st.error(f"❌ Error: {e}")
        return False

# --- 4. INTERFAZ WEB ---
st.title("🖥️ MIAA Monitor Web - Control de Sincronización")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4 = st.columns(4)
    
    with c1:
        modo = st.selectbox("Modo", ["Diario", "Periódico"], index=0)
    
    with c2:
        hora_input = st.number_input("Hora (0-23)", min_value=0, max_value=23, value=8)
    
    with c3:
        # Aquí puedes elegir cualquier minuto
        minuto_input = st.number_input("Minuto / Intervalo", min_value=0, max_value=59, value=0)
    
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        
        if not st.session_state.running:
            if st.button("▶️ INICIAR MONITOR", use_container_width=True):
                st.session_state.running = True
                st.rerun()
        else:
            if st.button("🛑 PARAR MONITOR", use_container_width=True, type="primary"):
                st.session_state.running = False
                st.rerun()

# --- 5. LÓGICA DE TIEMPO REAL ---
if st.session_state.running:
    ahora = datetime.datetime.now()
    
    if modo == "Diario":
        proximo = ahora.replace(hour=int(hora_input), minute=int(minuto_input), second=0, microsecond=0)
        if proximo <= ahora: proximo += datetime.timedelta(days=1)
    else:
        # Lógica de intervalo periódico (como en tu loop original)
        intervalo = int(minuto_input) if int(minuto_input) > 0 else 1
        prox_m = ((ahora.minute // intervalo) + 1) * intervalo
        proximo = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=prox_m)
    
    diff = proximo - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", f"{str(diff).split('.')[0]}")
    
    # Ejecución automática
    if diff.total_seconds() <= 5:
        ejecutar_actualizacion()
        st.write(f"Última ejecución: {ahora.strftime('%H:%M:%S')}")
        time.sleep(10) # Pausa para evitar bucle infinito en el mismo segundo
        st.rerun()
    
    # Botón manual por si acaso
    if st.button("🚀 Forzar Sincronización Ahora"):
        ejecutar_actualizacion()

    time.sleep(1) # Actualiza el reloj cada segundo
    st.rerun()
else:
    st.info("Estatus: DETENIDO. Configure el tiempo y presione INICIAR.")

st.divider()
st.subheader("Configuración SCADA Cargada")
st.json(MAPEO_SCADA)
