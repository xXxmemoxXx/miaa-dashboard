import streamlit as st
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine, text
import datetime
import time
import mysql.connector
import pytz

# --- 1. CONFIGURACIÓN DE ZONA HORARIA ---
zona_local = pytz.timezone('America/Mexico_City')

# --- 2. CONFIGURACIÓN DE PÁGINA ---
st.set_page_config(page_title="MIAA Control Center - Full Sync", layout="wide")

# --- 3. CONFIGURACIÓN DE CONEXIONES (Secrets) ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 4. MAPEO SCADA COMPLETO (ASEGÚRATE DE PEGAR AQUÍ TODOS TUS POZOS) ---
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


# --- 5. MAPEO POSTGRES COMPLETO ---
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

# --- 6. FUNCIONES TÉCNICAS Y LÓGICA DE NEGOCIO ---

def limpiar_dato_postgres(val):
    """Limpia formatos para evitar el error 'Double Precision' en Postgres."""
    if pd.isna(val) or val == "" or str(val).lower() == "nan": return None
    if isinstance(val, str):
        val = val.strip().replace(',', '')
        try: return float(val)
        except: return val
    return val

def obtener_gateids(tags):
    """Busca los IDs de los tags usando los nombres correctos de argumentos."""
    if not tags:
        return {}
        
    try:
        # Se eliminó la coma extra y se estructuró con 'with' para mayor seguridad
        with mysql.connector.connect(**DB_SCADA,) as conn:
            with conn.cursor(dictionary=True) as cursor:
                fmt = ','.join(['%s'] * len(tags))
                cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(tags))
                return {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
                
    except Exception as e: 
        print(f"⚠️ Error GATEIDs: {e}")
        return {}
    
def obtener_valores_scada(gateids):
    """Consulta masiva optimizada para 1500+ tags usando historial reciente."""
    if not gateids: return {}
    try:
        with mysql.connector.connect(**DB_SCADA) as conn:
            with conn.cursor(dictionary=True) as cursor:
                # Paso A: Obtener GATEIDs de los nombres de los tags
                fmt = ','.join(['%s'] * len(gateids))
                cursor.execute(f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({fmt})", list(gateids))
                tag_to_id = {r['NAME']: r['GATEID'] for r in cursor.fetchall()}
                
                if not tag_to_id: return {}
                
                # Paso B: Obtener últimos valores (VALUE) de vfitagnumhistory
                ids = list(tag_to_id.values())
                fmt_ids = ','.join(['%s'] * len(ids))
                query = f"""
                    SELECT GATEID, VALUE FROM vfitagnumhistory 
                    WHERE GATEID IN ({fmt_ids}) AND FECHA >= NOW() - INTERVAL 1 HOUR 
                    ORDER BY FECHA DESC
                """
                cursor.execute(query, ids)
                
                id_to_val = {}
                for r in cursor.fetchall():
                    if r['GATEID'] not in id_to_val:
                        id_to_val[r['GATEID']] = r['VALUE']
                
                return {name: id_to_val.get(gid) for name, gid in tag_to_id.items()}
    except Exception as e:
        st.error(f"⚠️ Error en SCADA: {e}")
        return {}

def ejecutar_actualizacion():
    try:
        with st.status("🚀 Iniciando Sincronización Total...", expanded=True) as status:
            # FASE 1: Descarga Google Sheets
            st.write("📥 Leyendo Google Sheets...")
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # FASE 2: Inyección de datos reales SCADA
            st.write("🔍 Consultando 1500+ variables en SCADA...")
            todos_los_tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
            vals_scada = obtener_valores_scada(todos_los_tags)
            
            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        if tag_scada in vals_scada:
                            val_real = vals_scada[tag_scada]
                            df.loc[mask, col_excel] = val_real
                            st.write(f"✅ {p_id}: {col_excel} -> {val_real}")

            # FASE 3: MySQL Informe
            st.write("💾 Actualizando MySQL...")
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                db_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df[[c for c in df.columns if c in db_cols]].to_sql('INFORME', con=conn, if_exists='append', index=False)

            # FASE 4: Postgres QGIS
            st.write("🐘 Sincronizando con Postgres (QGIS)...")
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

            status.update(label="✅ Sincronización Exitosa", state="complete")
            st.toast("¡Datos del SCADA inyectados correctamente!", icon="🚀")
    except Exception as e:
        st.error(f"❌ Error Crítico: {e}")

# --- 7. INTERFAZ WEB ---
st.title("🖥️ MIAA Data Center - Control Total")

with st.container(border=True):
    st.subheader("Configuración de Tiempo")
    c1, c2, c3, c4 = st.columns(4)
    with c1: 
        modo = st.selectbox("Modo", ["Diario", "Periódico"])
    with c2: 
        hora_in = st.number_input("Hora (0-23)", 0, 23, value=None, placeholder="Ej: 08")
    with c3: 
        min_in = st.number_input("Minuto / Intervalo", 0, 59, value=None, placeholder="Ej: 45")
    with c4:
        if "running" not in st.session_state: st.session_state.running = False
        btn_txt = "🛑 PARAR MONITOR" if st.session_state.running else "▶️ INICIAR MONITOR"
        if st.button(btn_txt, use_container_width=True, type="primary" if st.session_state.running else "secondary"):
            if not st.session_state.running and (hora_in is None or min_in is None):
                st.warning("⚠️ Ingresa los valores de tiempo.")
            else:
                st.session_state.running = not st.session_state.running
                st.rerun()

# --- 8. LÓGICA DE TIEMPO REAL ---
if st.session_state.running:
    ahora = datetime.datetime.now(zona_local)
    h, m = (hora_in or 0), (min_in or 0)
    
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
    st.info("Estatus: Detenido. Ajuste el tiempo e inicie.")

if st.button("🚀 Forzar Sincronización Manual"):
    ejecutar_actualizacion()

