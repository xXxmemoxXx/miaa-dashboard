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

# --- 1. CONFIGURACIÓN DE CONEXIONES (Basado en respaldo) ---
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

def limpiar_numero(valor):
    if pd.isna(valor) or valor == "": return None
    try:
        if isinstance(valor, str): return float(valor.replace(',', '').strip())
        return float(valor)
    except: return valor

def obtener_gateids(tags):
    """Busca los IDs de los tags usando los nombres correctos de argumentos."""
    if not tags:
        return {}
        
    try:
        # Se eliminó la coma extra y se estructuró con 'with' para mayor seguridad
        with mysql.connector.connect(**DB_SCADA, use_pure=True, autocommit=True) as conn:
            with conn.cursor() as cursor:
                format_strings = ','.join(['%s'] * len(tags))
                query = f"SELECT NAME, GATEID FROM VfiTagRef WHERE NAME IN ({format_strings})"
                
                cursor.execute(query, list(tags))
                return {name: gid for name, gid in cursor.fetchall()}
                
    except Exception as e: 
        print(f"⚠️ Error GATEIDs: {e}")
        return {}

def obtener_valores_scada(gateids):
    """Obtiene valores de los últimos 15 min por ID."""
    if not gateids: 
        return {}
        
    try:
        # Conexión con los parámetros solicitados y uso de 'with' para auto-cierre
        with mysql.connector.connect(**DB_SCADA, use_pure=True, autocommit=True) as conn:
            with conn.cursor() as cursor:
                ids = list(gateids.values())
                format_strings = ','.join(['%s'] * len(ids))
                
                query = f"""
                    SELECT GATEID, VALUE 
                    FROM vfitagnumhistory 
                    WHERE GATEID IN ({format_strings}) 
                      AND FECHA >= NOW() - INTERVAL 15 MINUTE 
                    ORDER BY FECHA DESC
                """
                
                cursor.execute(query, ids)
                
                data = {}
                # Al estar ordenado por FECHA DESC, el primer 'gid' que guardamos
                # en el diccionario es el más nuevo (el último registrado).
                for gid, val in cursor.fetchall():
                    if gid not in data:
                        data[gid] = val
                        
                return data

    except Exception as e: 
        print(f"⚠️ Error SCADA: {e}")
        return {}

# =================================================================
# 3. PROCESO DE ACTUALIZACIÓN
# =================================================================

def ejecutar_actualizacion(app_ref):
    def ui_upd(val):
        app_ref.root.after(0, lambda: app_ref.progress.config(value=val))
        app_ref.root.after(0, lambda: app_ref.lbl_porcentaje.config(text=f"{val}%"))

    try:
        # FASE 1: GOOGLE SHEETS (10-30%)
        ui_upd(10)
        df = pd.read_csv(CSV_URL)
        df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
        print(f"✅ Google Sheets: {len(df)} registros leídos.")
        ui_upd(30)

        # FASE 2: MYSQL INFORME (40-60%)
        print("💾 Actualizando MySQL...")
        ui_upd(40)
        pass_my = urllib.parse.quote_plus(DB_INFORME['password'])
        # Cambiamos a mysql+mysqlconnector explícitamente
        engine_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{pass_my}@{DB_INFORME['host']}/{DB_INFORME['database']}", pool_pre_ping=True)
        
        with engine_my.begin() as conn:
            conn.execute(text("TRUNCATE TABLE INFORME"))
            res_cols = conn.execute(text("SHOW COLUMNS FROM INFORME"))
            db_cols = [r[0] for r in res_cols]
            df_to_save = df[[c for c in df.columns if c in db_cols]].copy()
            df_to_save.to_sql('INFORME', con=conn, if_exists='append', index=False)
        
        engine_my.dispose()
        print("✅ MySQL: Actualizado.")
        ui_upd(60)

        # --- D. Sincronización PostgreSQL (QGIS) ---
        ui_upd(80)
        print("🐘 Conectando a PostgreSQL (QGIS)...")

        # Aseguramos que la contraseña no rompa la URL
        pass_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        url_pg = f"postgresql+psycopg2://{DB_POSTGRES['user']}:{pass_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}"

        # Configuramos el motor con parámetros de estabilidad para ejecutables
        engine_pg = create_engine(
            url_pg, 
            pool_pre_ping=True, 
            connect_args={'connect_timeout': 10}
        )

        updates_pg = 0
        try:
            with engine_pg.connect() as conn:
                print("🔗 Conexión establecida. Iniciando transacción...")
                with conn.begin(): 
                    for _, row in df.iterrows():
                        id_maestro = str(row['ID']).strip() if 'ID' in row else None
                        if not id_maestro or id_maestro.lower() == "nan":
                            continue

                        set_clauses = []
                        params = {"id": id_maestro}

                        for col_csv, col_pg in MAPEO_POSTGRES.items():
                            if col_csv in df.columns:
                                val = row[col_csv]
                                
                                # Limpieza estricta de datos
                                if pd.isna(val) or val == "":
                                    val = None
                                elif isinstance(val, str):
                                    val = val.strip()
                                    clean_val = val.replace(',', '')
                                    if clean_val.replace('.', '', 1).isdigit():
                                        try: val = float(clean_val)
                                        except: pass
                                
                                set_clauses.append(f'"{col_pg}" = :{col_pg}')
                                params[col_pg] = val

                        if set_clauses:
                            sql = text(f'UPDATE public."Pozos" SET {", ".join(set_clauses)} WHERE "ID" = :id')
                            res = conn.execute(sql, params)
                            updates_pg += res.rowcount

                print(f"✅ PostgreSQL: {updates_pg} registros sincronizados.")
                ui_upd(100)

        except Exception as e:
            print(f"❌ Error en Postgres: {type(e).__name__} -> {str(e)}")
        finally:
            engine_pg.dispose()

    except Exception as e:
        print(f"❌ Error General: {str(e)}")

# --- 5. INTERFAZ WEB ---
st.title("🖥️ MIAA Control Center")

with st.container(border=True):
    st.subheader("Panel de Control (Hora México)")
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
    st.write(f"🕒 Hora local: **{ahora.strftime('%H:%M:%S')}**")
    st.metric("⏳ SIGUIENTE SINCRONIZACIÓN EN:", f"{str(diff).split('.')[0]}")

    if diff.total_seconds() <= 5:
        ejecutar_actualizacion()
        time.sleep(10); st.rerun()
    
    time.sleep(1); st.rerun()
else:
    st.info("Estatus: Monitor detenido.")

if st.button("🚀 Sincronizar Ahora (Manual)"):
    ejecutar_actualizacion()
