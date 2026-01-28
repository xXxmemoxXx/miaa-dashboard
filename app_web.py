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
st.set_page_config(page_title="MIAA Control Center - Full Sync", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEO SCADA (Asegúrate de tener todos tus pozos aquí) ---
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
    },
    "P-003": {
        "GASTO_(l.p.s.)":"PZ_003_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_003_PRES_INS",
        "VOLTAJE_L1":"PZ_003_VOL_L1_L2",
        "VOLTAJE_L2":"PZ_003_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_003_VOL_L1_L3",
        "AMP_L1":"PZ_003_CORR_L1",
        "AMP_L2":"PZ_003_CORR_L2",
        "AMP_L3":"PZ_003_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_003_LONG_COLUM",
        "SUMERGENCIA":"PZ_003_SUMERG",
        "NIVEL_DINAMICO":"PZ_003_NIV_EST",
    },
    "P-004": {
        "GASTO_(l.p.s.)":"PZ_004_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_004_PRES_INS",
        "VOLTAJE_L1":"PZ_004_VOL_L1_L2",
        "VOLTAJE_L2":"PZ_004_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_004_VOL_L1_L3",
        "AMP_L1":"PZ_004_CORR_L1",
        "AMP_L2":"PZ_004_CORR_L2",
        "AMP_L3":"PZ_004_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_004_LONG_COLUM",
        "SUMERGENCIA":"PZ_004_SUMERG",
        "NIVEL_DINAMICO":"PZ_004_NIV_EST",
    },
    "P-005A": {
        "GASTO_(l.p.s.)":"PZ_RP_005_TRHDAS_CAU_INS",
        "PRESION_(kg/cm2)":"PZ_RP_005_TRHDAS_PRES_INS",
        "VOLTAJE_L1":"PZ_RP_005_TRHDAS_VOL_L1_L2",
        "VOLTAJE_L2":"PZ_RP_005_TRHDAS_VOL_L2_L3",
        "VOLTAJE_L3":"PZ_RP_005_TRHDAS_VOL_L1_L3",
        "AMP_L1":"PZ_RP_005_TRHDAS_CORR_L1",
        "AMP_L2":"PZ_RP_005_TRHDAS_CORR_L2",
        "AMP_L3":"PZ_RP_005_TRHDAS_CORR_L3",
        "LONGITUD_DE_COLUMNA":"PZ_RP_005_TRHDAS_LONG_COLUM",
        "SUMERGENCIA":"PZ_RP_005_TRHDAS_SUMERG",
        "NIVEL_DINAMICO":"PZ_RP_005_TRHDAS_NIV_EST",
    },
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 'PRESION_(kg/cm2)': '_Presion',
    'LONGITUD_DE_COLUMNA': '_Long_colum', 'NIVEL_DINAMICO_(mts)': '_Nivel_Din',
    'ESTATUS': '_Estatus', 'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. FUNCIONES ---

def limpiar_dato_postgres(val):
    if pd.isna(val) or val == "" or str(val).lower() == "nan": return None
    if isinstance(val, str):
        val = val.strip().replace(',', '')
        try: return float(val)
        except: return val
    return val

def obtener_valores_scada(tags):
    if not tags: return {}
    try:
        with mysql.connector.connect(**DB_SCADA) as conn:
            with conn.cursor(dictionary=True) as cursor:
                fmt = ','.join(['%s'] * len(tags))
                cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(tags))
                tag_to_id = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
                if not tag_to_id: return {}
                
                ids = list(tag_to_id.values())
                fmt_ids = ','.join(['%s'] * len(ids))
                query = f"SELECT GATEID, VALUE FROM vfitagnumhistory WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR ORDER BY FECHA DESC"
                cursor.execute(query, ids)
                
                id_to_val = {}
                for r in cursor.fetchall():
                    if r['GATEID'] not in id_to_val: id_to_val[r['GATEID']] = r['VALUE']
                return {name: id_to_val.get(gid) for name, gid in tag_to_id.items()}
    except Exception as e:
        st.error(f"Error SCADA: {e}")
        return {}

def ejecutar_actualizacion():
    try:
        with st.status("🔄 Sincronizando datos (Prioridad SCADA)...", expanded=True) as status:
            # 1. Cargar Base Informe (Google Sheets)
            st.write("📥 Leyendo Google Sheets...")
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # 2. Obtener datos de SCADA
            st.write("🔍 Consultando variables SCADA...")
            todos_tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
            vals_scada = obtener_valores_scada(todos_tags)
            
            # 3. SOBREESCRIBIR DATOS (Aquí garantizamos que SCADA mande sobre el Informe)
            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        if tag_scada in vals_scada:
                            val_real = vals_scada[tag_scada]
                            # Forzamos el valor real en el DataFrame antes de cualquier guardado
                            df.loc[mask, col_excel] = val_real

            # 4. MySQL Informe (Guardamos el DF ya corregido con SCADA)
            st.write("💾 Actualizando MySQL Informe...")
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                db_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df_to_save = df[[c for c in df.columns if c in db_cols]].copy()
                df_to_save.to_sql('INFORME', con=conn, if_exists='append', index=False)

            # 5. Postgres QGIS
            st.write("🐘 Sincronizando Postgres (QGIS)...")
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        id_val = str(row['ID']).strip()
                        sets, params = [], {"id": id_val}
                        for c_csv, c_pg in MAPEO_POSTGRES.items():
                            if c_csv in df.columns:
                                val = limpiar_dato_postgres(row[c_csv])
                                sets.append(f'"{c_pg}" = :{c_pg}'); params[c_pg] = val
                        if sets:
                            conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)

            status.update(label="✅ Sincronización Exitosa: SCADA inyectado.", state="complete")
            st.success("Los datos de SCADA han sido priorizados y guardados.")
    except Exception as e:
        st.error(f"Error Crítico: {e}")

# --- 5. INTERFAZ ---
tab_monitor, tab_postgres = st.tabs(["🚀 Monitor", "🔍 Visor Postgres"])

with tab_monitor:
    st.header("Control de Carga Automática")
    with st.container(border=True):
        c1, c2, c3, c4 = st.columns(4)
        with c1: modo = st.selectbox("Modo", ["Diario", "Periódico"])
        with c2: hora_in = st.number_input("Hora", 0, 23, value=None, placeholder="HH")
        with c3: min_in = st.number_input("Min/Int", 0, 59, value=None, placeholder="MM")
        with c4:
            if "running" not in st.session_state: st.session_state.running = False
            if st.button("🛑 PARAR" if st.session_state.running else "▶️ INICIAR", use_container_width=True):
                st.session_state.running = not st.session_state.running
                st.rerun()

    # BOTÓN DE ACTUALIZAR MANUAL (RESTABLECIDO)
    if st.button("🔄 FORZAR ACTUALIZACIÓN MANUAL (SCADA -> DBs)", type="primary"):
        ejecutar_actualizacion()

    if st.session_state.running:
        ahora = datetime.datetime.now(zona_local)
        st.write(f"🕒 Monitor activo. Hora local: **{ahora.strftime('%H:%M:%S')}**")
        # Aquí iría la lógica de diff y proximo para el auto-sync
        time.sleep(1); st.rerun()

with tab_postgres:
    st.header("Datos actuales en QGIS (Postgres)")
    if st.button("🔄 Refrescar Tabla"):
        try:
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            df_pg = pd.read_sql('SELECT * FROM public."Pozos" ORDER BY "ID" ASC', eng_pg)
            st.dataframe(df_pg, use_container_width=True)
        except Exception as e:
            st.error(f"Error: {e}")
