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

# --- 1. CONFIGURACIÓN DE CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 2. MAPEO SCADA COMPLETO ---
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

# --- 3. MAPEO POSTGRES COMPLETO ---
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

# --- 4. FUNCIONES DE LIMPIEZA Y LÓGICA ---
def limpiar_dato_postgres(valor):
    """Quita comas y convierte a float si es necesario para Postgres."""
    if pd.isna(valor) or valor == "" or str(valor).lower() == "nan":
        return None
    try:
        if isinstance(valor, str):
            # Quitar comas de miles y espacios
            limpio = valor.replace(',', '').strip()
            return float(limpio)
        return float(valor)
    except:
        return valor # Si no es número (ej. 'En servicio'), se queda como está

def ejecutar_actualizacion():
    try:
        with st.status("🔄 Iniciando proceso de sincronización...", expanded=True) as status:
            # FASE 1: GOOGLE SHEETS
            st.write("📥 Descargando datos de Google Sheets...")
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]

            # FASE 2: MYSQL INFORME
            st.write("💾 Actualizando MySQL Informe...")
            pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
            engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with engine_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                res_cols = conn.execute(text("SHOW COLUMNS FROM INFORME"))
                db_cols = [r[0] for r in res_cols]
                df_to_save = df[[c for c in df.columns if c in db_cols]].copy()
                df_to_save.to_sql('INFORME', con=conn, if_exists='append', index=False)
            
            # FASE 3: POSTGRESQL (QGIS)
            st.write("🐘 Sincronizando PostgreSQL (QGIS)...")
            pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            url_pg = f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}"
            engine_pg = create_engine(url_pg)

            with engine_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        id_m = str(row['ID']).strip() if 'ID' in row else None
                        if not id_m or id_m.lower() == "nan": continue
                        
                        set_clauses = []
                        params = {"id": id_m}
                        for col_csv, col_pg in MAPEO_POSTGRES.items():
                            if col_csv in df.columns:
                                val = limpiar_dato_postgres(row[col_csv])
                                set_clauses.append(f'"{col_pg}" = :{col_pg}')
                                params[col_pg] = val
                        
                        if set_clauses:
                            sql = text(f'UPDATE public."Pozos" SET {", ".join(set_clauses)} WHERE "ID" = :id')
                            conn.execute(sql, params)

            status.update(label="✅ Sincronización Exitosa", state="complete")
            st.toast("MySQL y Postgres actualizados correctamente.", icon="🚀")
    except Exception as e:
        st.error(f"❌ Error crítico: {e}")

# --- 5. INTERFAZ WEB ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    st.subheader("Panel de Configuración")
    c1, c2, c3, c4 = st.columns(4)
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: hora_in = st.number_input("Hora (0-23)", 0, 23, 12)
    with c3: min_in = st.number_input("Minuto / Intervalo", 0, 59, 0)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR"
        if st.button(btn_label, use_container_width=True, type="primary" if st.session_state.running else "secondary"):
            st.session_state.running = not st.session_state.running
            st.rerun()

# --- 6. LÓGICA DE TIEMPO REAL ---
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    if modo == "Diario":
        proximo = ahora.replace(hour=int(hora_in), minute=int(min_in), second=0, microsecond=0)
        if proximo <= ahora: proximo += datetime.timedelta(days=1)
    else:
        intervalo = int(min_in) if int(min_in) > 0 else 1
        prox_m = ((ahora.minute // intervalo) + 1) * intervalo
        if prox_m >= 60: proximo = ahora.replace(minute=0, second=0, microsecond=0) + datetime.timedelta(hours=1)
        else: proximo = ahora.replace(minute=prox_m, second=0, microsecond=0)

    diff = proximo - ahora
    st.write(f"🕒 Hora actual: **{ahora.strftime('%H:%M:%S')}**")
    st.metric("⏳ PRÓXIMA CARGA EN:", f"{str(diff).split('.')[0]}")

    if diff.total_seconds() <= 5:
        ejecutar_actualizacion()
        time.sleep(10); st.rerun()
    
    time.sleep(1); st.rerun()
else:
    st.info("Estatus: Detenido.")

if st.button("🚀 Forzar Sincronización"): ejecutar_actualizacion()

with st.expander("Ver Mapeos"):
    st.write("SCADA:", MAPEO_SCADA)
    st.write("Postgres:", MAPEO_POSTGRES)
