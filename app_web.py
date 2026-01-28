import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector
import pytz

# --- CONFIGURACIÓN DE ZONA HORARIA ---
zona_local = pytz.timezone('America/Mexico_City')

# --- CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA Control Center", layout="wide")

# --- 1. CONFIGURACIÓN DE CONEXIONES (Uso de Secrets) ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 2. MAPEOS (SCADA Y POSTGRES) ---
# Aquí incluyes el bloque completo de tus 1500+ variables
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
    # Continuar con el resto de los pozos...
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'COLUMNA_DIAMETRO_1': '_Diam_colum',
    'TIPO_COLUMNA': '_Tipo_colum', 'SECTOR_HIDRAULICO': '_Sector',
    'NIVEL_DINAMICO_(mts)': '_Nivel_Din', 'NIVEL_ESTATICO_(mts)': '_Nivel_Est',
    'EXTRACCION_MENSUAL_(m3)': '_Vm_estr', 'HORAS_DE_OPERACIÓN_DIARIA_(hrs)': '_Horas_op',
    'DISTRITO_1': '_Distrito', 'ESTATUS': '_Estatus',
    'TELEMETRIA': '_Telemetria', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 3. LÓGICA DE PROCESAMIENTO ---

def limpiar_dato_postgres(val):
    """Evita el error de 'double precision' limpiando comas."""
    if pd.isna(val) or val == "" or str(val).lower() == "nan": return None
    if isinstance(val, str):
        val = val.strip().replace(',', '')
        try: return float(val)
        except: return val
    return val

def obtener_valores_scada(tags):
    """Consulta masiva optimizada para 1500+ variables."""
    try:
        with mysql.connector.connect(**DB_SCADA) as conn:
            with conn.cursor(dictionary=True) as cursor:
                format_strings = ','.join(['%s'] * len(tags))
                query = f"SELECT NAME, VAL FROM VfiTagRef WHERE NAME IN ({format_strings})"
                cursor.execute(query, tags)
                return {row['NAME']: row['VAL'] for row in cursor.fetchall()}
    except Exception as e:
        st.error(f"Error SCADA: {e}")
        return {}

def ejecutar_actualizacion():
    try:
        with st.status("🚀 Iniciando Sincronización Masiva...", expanded=True) as status:
            # Google Sheets
            st.write("📥 Leyendo Excel...")
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]

            # SCADA Integration
            st.write("🔍 Actualizando datos desde SCADA...")
            todos_tags = [tag for p in MAPEO_SCADA.values() for tag in p.values()]
            vals = obtener_valores_scada(todos_tags)
            
            for p_id, m in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_ex, tag in m.items():
                        if tag in vals: df.loc[mask, col_ex] = vals[tag]

            # MySQL Informe
            st.write("💾 Guardando en MySQL...")
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                df_save = df[[c for c in df.columns if c in [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]]].copy()
                df_save.to_sql('INFORME', con=conn, if_exists='append', index=False)

            # Postgres QGIS
            st.write("🐘 Sincronizando con QGIS...")
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        id_m = str(row['ID']).strip()
                        if not id_m or id_m == "nan": continue
                        sets, params = [], {"id": id_m}
                        for c_csv, c_pg in MAPEO_POSTGRES.items():
                            if c_csv in df.columns:
                                val = limpiar_dato_postgres(row[c_csv])
                                sets.append(f'"{c_pg}" = :{c_pg}'); params[c_pg] = val
                        if sets:
                            conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)

            status.update(label="✅ Todo actualizado correctamente", state="complete")
    except Exception as e:
        st.error(f"Error: {e}")

# --- 4. INTERFAZ WEB (CUADROS VACÍOS) ---
st.title("🖥️ MIAA Data Center - Sync Engine")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4 = st.columns(4)
    with c1: 
        modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: 
        # Al poner value=None, el cuadro aparece vacío
        hora_in = st.number_input("Hora (0-23)", min_value=0, max_value=23, value=None, placeholder="Ej: 14")
    with c3: 
        # Al poner value=None, el cuadro aparece vacío
        min_in = st.number_input("Minuto / Intervalo", min_value=0, max_value=59, value=None, placeholder="Ej: 30")
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
            if not st.session_state.running and (hora_in is None or min_in is None):
                st.warning("⚠️ Por favor ingresa hora y minuto antes de iniciar.")
            else:
                st.session_state.running = not st.session_state.running
                st.rerun()

# --- 5. MONITOR DE TIEMPO ---
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    # Convertimos None a 0 solo para el cálculo si es necesario
    h = hora_in if hora_in is not None else 0
    m = min_in if min_in is not None else 0
    
    if modo == "Diario":
        proximo = ahora.replace(hour=int(h), minute=int(m), second=0, microsecond=0)
        if proximo <= ahora: proximo += datetime.timedelta(days=1)
    else:
        intervalo = int(m) if int(m) > 0 else 1
        prox_m = ((ahora.minute // intervalo) + 1) * intervalo
        if prox_m >= 60: proximo = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        else: proximo = ahora.replace(minute=prox_m, second=0, microsecond=0)

    diff = proximo - ahora
    st.write(f"🕒 Hora local: **{ahora.strftime('%H:%M:%S')}**")
    st.metric("⏳ SIGUIENTE CARGA EN:", f"{str(diff).split('.')[0]}")

    if diff.total_seconds() <= 5:
        ejecutar_actualizacion()
        time.sleep(10); st.rerun()
    
    time.sleep(1); st.rerun()
else:
    st.info("Estatus: Monitor detenido.")

if st.button("🚀 Forzar Sync Manual"): 
    ejecutar_actualizacion()
