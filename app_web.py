import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector
import pytz
import numpy as np

# --- 1. CONFIGURACIÓN ---
zona_local = pytz.timezone('America/Mexico_City')
st.set_page_config(page_title="MIAA Control Maestro", layout="wide")

# Credenciales
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': 'h97_p,NQPo=l', 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': 'bWkrw1Uum1O&', 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': 'M144.Tec', 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}
CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx:out:csv&sheet=informe'

# Mapeo Completo Integrado
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
    'FECHA_ACTUALIZACION':             '_Ultima_actualizacion'
}

MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)":"PZ_002_TRC_CAU_INS", "PRESION_(kg/cm2)":"PZ_002_TRC_PRES_INS",
        "VOLTAJE_L1":"PZ_002_TRC_VOL_L1_L2", "AMP_L1":"PZ_002_TRC_CORR_L1",
        "LONGITUD_DE_COLUMNA":"PZ_002_TRC_LONG_COLUM", "NIVEL_DINAMICO_(mts)":"PZ_002_TRC_NIV_EST",
    },
    "P-003": {
        "GASTO_(l.p.s.)":"PZ_003_CAU_INS", "PRESION_(kg/cm2)":"PZ_003_PRES_INS",
        "VOLTAJE_L1":"PZ_003_VOL_L1_L2", "AMP_L1":"PZ_003_CORR_L1",
        "LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM", "NIVEL_DINAMICO_(mts)":"PZ_003_NIV_EST",
    }
}

# --- 2. LÓGICA DE PROCESAMIENTO ---

def ejecutar_sincronizacion_total():
    st.session_state.last_logs = [] # Limpiar consola al iniciar
    logs = []
    progreso_bar = st.progress(0)
    status_text = st.empty()
    filas_pg = 0
    
    try:
        # 1. Google Sheets
        status_text.text("Leyendo datos maestros...")
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        if 'FECHA_ACTUALIZACION' in df.columns:
            df['FECHA_ACTUALIZACION'] = pd.to_datetime(df['FECHA_ACTUALIZACION'], errors='coerce')
        logs.append(f"✅ Google Sheets: {len(df)} registros leídos.")
        progreso_bar.progress(20)

        # 2. SCADA
        status_text.text("Consultando SCADA...")
        conn_s = mysql.connector.connect(**DB_SCADA)
        all_tags = []
        for p_id in MAPEO_SCADA: all_tags.extend(MAPEO_SCADA[p_id].values())
        
        query = f"SELECT r.NAME, h.VALUE FROM vfitagnumhistory h JOIN VfiTagRef r ON h.GATEID = r.GATEID WHERE r.NAME IN ({','.join(['%s']*len(all_tags))}) AND h.FECHA >= NOW() - INTERVAL 1 DAY ORDER BY h.FECHA DESC"
        df_scada = pd.read_sql(query, conn_s, params=all_tags).drop_duplicates('NAME')
        
        for p_id, config in MAPEO_SCADA.items():
            for col_excel, tag_name in config.items():
                val = df_scada.loc[df_scada['NAME'] == tag_name, 'VALUE']
                if not val.empty:
                    df.loc[df['POZOS'] == p_id, col_excel] = round(float(val.values[0]), 2)
        conn_s.close()
        logs.append("🧬 SCADA: Valores inyectados correctamente.")
        progreso_bar.progress(50)

        # 3. MySQL Informe
        status_text.text("Actualizando MySQL...")
        p_my = urllib.parse.quote_plus(DB_INFORME['password'])
        eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
        with eng_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            df_sql = df.replace({np.nan: None, pd.NaT: None})
            df_sql.to_sql('INFORME', con=conn, if_exists='append', index=False)
        logs.append("✅ MySQL: Tabla INFORME actualizada.")
        progreso_bar.progress(75)

        # 4. Postgres QGIS
        status_text.text("Sincronizando con QGIS...")
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        
        with eng_pg.begin() as conn:
            for _, row in df.iterrows():
                id_val = str(row['ID']).strip() if pd.notnull(row['ID']) else None
                if id_val and id_val != "nan":
                    params = {'id': id_val}
                    sets = []
                    for csv_col, pg_col in MAPEO_POSTGRES.items():
                        if csv_col in df.columns:
                            val = row[csv_col]
                            # LIMPIEZA DE DATOS (Solución al error de tu imagen)
                            if pd.isna(val) or val == 'NaN':
                                clean_val = None
                            elif pg_col == '_Ultima_actualizacion':
                                clean_val = val.to_pydatetime() if hasattr(val, 'to_pydatetime') else val
                            elif isinstance(val, str):
                                # Quitar comas de números en texto (ej. "50,399.00" -> 50399.00)
                                clean_val = val.replace(',', '')
                                try: clean_val = float(clean_val)
                                except: pass # Mantener como string si es ESTATUS o SECTOR
                            else:
                                clean_val = val
                                
                            params[pg_col] = clean_val
                            sets.append(f'"{pg_col}" = :{pg_col}')
                    
                    if sets:
                        res = conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)
                        filas_pg += res.rowcount
        
        logs.append(f"🐘 Postgres: Tabla POZOS actualizada ({filas_pg} filas).")
        logs.append(f"🚀 SINCRO EXITOSA: {datetime.datetime.now(zona_local).strftime('%H:%M:%S')}")
        progreso_bar.progress(100)
        status_text.empty()
        return logs
    except Exception as e:
        return [f"❌ Error crítico: {str(e)}"]

# --- 3. INTERFAZ ---

def reset_console():
    st.session_state.last_logs = ["SISTEMA EN ESPERA (Configuración actualizada)..."]

st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    c1, c2, c3, c4, c5 = st.columns([1.5, 1, 1, 1.5, 1.5])
    with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"], index=0, on_change=reset_console)
    with c2: h_in = st.number_input("Hora", 0, 23, value=12, on_change=reset_console)
    with c3: m_in = st.number_input("Min/Int", 0, 59, value=30, on_change=reset_console)
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_label = "🛑 PARAR" if st.session_state.running else "▶️ INICIAR"
        if st.button(btn_label, use_container_width=True):
            st.session_state.running = not st.session_state.running
            st.rerun()
    with c5:
        if st.button("🚀 FORZAR CARGA", use_container_width=True):
            st.session_state.last_logs = ejecutar_sincronizacion_total()

log_txt = "<br>".join(st.session_state.get('last_logs', ["SISTEMA EN ESPERA..."]))
st.markdown(f'<div style="background-color:black;color:#00FF00;padding:15px;font-family:Consolas;height:250px;overflow-y:auto;border-radius:5px;line-height:1.6;">{log_txt}</div>', unsafe_allow_html=True)

# --- 4. RELOJ UNIFICADO ---
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    if modo == "Diario":
        prox = ahora.replace(hour=h_in, minute=m_in, second=0, microsecond=0)
        if ahora >= prox: prox += datetime.timedelta(days=1)
    else:
        total_m = ahora.hour * 60 + ahora.minute
        sig = ((total_m // m_in) + 1) * m_in
        prox = ahora.replace(hour=0, minute=0, second=0, microsecond=0) + datetime.timedelta(minutes=sig)

    diff = prox - ahora
    st.metric("⏳ PRÓXIMA CARGA EN:", str(diff).split('.')[0])
    
    if diff.total_seconds() <= 1:
        st.session_state.last_logs = ejecutar_sincronizacion_total()
        st.rerun()
    
    time.sleep(1)
    st.rerun()
