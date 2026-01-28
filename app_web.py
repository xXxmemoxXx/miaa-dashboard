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

# --- 2. MAPEOS (Originales de tu código) ---
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


# --- 3. FUNCIONES DE LÓGICA SCADA ---
def obtener_gateids(tags):
    try:
        with mysql.connector.connect(**DB_SCADA) as conn:
            with conn.cursor() as cursor:
                format_strings = ','.join(['%s'] * len(tags))
                query = f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({format_strings})"
                cursor.execute(query, list(tags))
                return {name: gid for name, gid in cursor.fetchall()}
    except Exception as e:
        st.error(f"Error GATEIDs: {e}")
        return {}

# --- 4. PROCESO DE ACTUALIZACIÓN ---
def ejecutar_actualizacion():
    try:
        # FASE 1: Obtener Datos SCADA (Usando tu MAPEO_SCADA)
        st.write("🔍 Consultando tags en SCADA...")
        tags_a_buscar = []
        for pozo in MAPEO_SCADA.values():
            tags_a_buscar.extend(pozo.values())
        
        gids = obtener_gateids(tags_a_buscar)
        st.write(f"✅ Se encontraron {len(gids)} identificadores de tags.")

        # FASE 2: Google Sheets y MySQL
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        
        pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
        engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        
        with engine_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            res_cols = conn.execute(text("SHOW COLUMNS FROM INFORME"))
            db_cols = [r[0] for r in res_cols]
            df_to_save = df[[c for c in df.columns if c in db_cols]].copy()
            df_to_save.to_sql('INFORME', con=conn, if_exists='append', index=False)
        
        st.toast("🚀 Sincronización exitosa", icon="✅")
        return True
    except Exception as e:
        st.error(f"Error General: {e}")
        return False

# --- 5. INTERFAZ GRÁFICA (Front-end) ---
st.title("🖥️ MIAA Data Center - Monitor Web")

# Contenedor de Configuración (Equivalente a tu LabelFrame)
with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4 = st.columns(4)
    
    with c1:
        modo = st.selectbox("Modo", ["Diario", "Periódico"], index=0)
    with c2:
        hora = st.number_input("Hora (0-23)", 0, 23, 8) if modo == "Diario" else 0
    with c3:
        # Ahora puedes elegir CUALQUIER minuto (0-59) o intervalo
        min_label = "Minuto exacto (0-59)" if modo == "Diario" else "Intervalo (Minutos)"
        minuto_val = st.number_input(min_label, min_value=1, max_value=59, value=10)
    
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        
        if not st.session_state.running:
            if st.button("▶️ INICIAR", use_container_width=True):
                st.session_state.running = True
                st.rerun()
        else:
            if st.button("🛑 PARAR", use_container_width=True, type="primary"):
                st.session_state.running = False
                st.rerun()

# --- LÓGICA DEL TEMPORIZADOR ---
if st.session_state.running:
    ahora = datetime.datetime.now()
    if modo == "Diario":
        proximo = ahora.replace(hour=int(hora), minute=int(min_int), second=0, microsecond=0)
        if proximo <= ahora: proximo += datetime.timedelta(days=1)
    else:
        prox_m = ((ahora.minute // int(min_int)) + 1) * int(min_int)
        proximo = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=prox_m)
    
    diff = proximo - ahora
    st.metric("Próxima Carga En:", f"{str(diff).split('.')[0]}")
    
    # Simulación de log
    st.text_area("Registro de Actividad", value=f"[{ahora.strftime('%H:%M:%S')}] Monitor activo esperando a {proximo.strftime('%H:%M:%S')}", height=100)
    
    time.sleep(10) # Frecuencia de actualización de la página
    st.rerun()
else:

    st.warning("Estatus: DETENIDO")
