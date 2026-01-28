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
st.set_page_config(page_title="MIAA Control Center - Smart Sync", layout="wide")

# --- 2. CONEXIONES ---
DB_SCADA = {'host': 'miaa.mx', 'user': 'miaamx_dashboard', 'password': st.secrets["db_scada"]["password"], 'database': 'miaamx_telemetria'}
DB_INFORME = {'host': 'miaa.mx', 'user': 'miaamx_telemetria2', 'password': st.secrets["db_informe"]["password"], 'database': 'miaamx_telemetria2'}
DB_POSTGRES = {'user': 'map_tecnica', 'pass': st.secrets["db_postgres"]["pass"], 'host': 'ti.miaa.mx', 'db': 'qgis', 'port': 5432}

CSV_URL = 'https://docs.google.com/spreadsheets/d/1tHh47x6DWZs_vCaSCHshYPJrQKUW7Pqj86NCVBxKnuw/gviz/tq?tqx=out:csv&sheet=informe'

# --- 3. MAPEOS ---
MAPEO_SCADA = {
    "P-002": {
        "GASTO_(l.p.s.)": "PZ_002_TRC_CAU_INS",
        "PRESION_(kg/cm2)": "PZ_002_TRC_PRES_INS",
        "NIVEL_DINAMICO": "PZ_002_TRC_NIV_EST"
    }
}

MAPEO_POSTGRES = {
    'GASTO_(l.p.s.)': '_Caudal', 
    'PRESION_(kg/cm2)': '_Presion',
    'NIVEL_DINAMICO_(mts)': '_Nivel_Din',
    'ESTATUS': '_Estatus', 
    'FECHA_ACTUALIZACION': '_Ultima_actualizacion'
}

# --- 4. FUNCIONES ---

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
                # Buscamos el último valor histórico
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
        with st.status("🔄 Sincronizando con Lógica de Respaldo...", expanded=True) as status:
            # 1. Leer Google Sheets (Base de respaldo)
            df = pd.read_csv(CSV_URL)
            df.columns = [col.strip().replace('\n', ' ') for col in df.columns]
            df['ID'] = df['ID'].astype(str).str.strip()

            # 2. Consultar SCADA
            todos_tags = [t for p in MAPEO_SCADA.values() for t in p.values()]
            vals_scada = obtener_valores_scada(todos_tags)
            
            # 3. APLICAR LÓGICA: SCADA > 0 ? SCADA : SHEETS
            for p_id, mapeo in MAPEO_SCADA.items():
                mask = df['ID'] == p_id
                if mask.any():
                    for col_excel, tag_scada in mapeo.items():
                        val_scada = vals_scada.get(tag_scada)
                        
                        # Si SCADA tiene un valor válido y es mayor a 0, sobreescribimos
                        if val_scada is not None and float(val_scada) > 0:
                            df.loc[mask, col_excel] = val_scada
                            st.write(f"📡 {p_id}: Usando SCADA ({val_scada}) para {col_excel}")
                        else:
                            st.write(f"📝 {p_id}: SCADA en 0 o nulo. Manteniendo valor de Sheets para {col_excel}")

            # 4. MySQL Informe
            p_my = urllib.parse.quote_plus(DB_INFORME['password'])
            eng_my = create_engine(f"mysql+mysqlconnector://{DB_INFORME['user']}:{p_my}@{DB_INFORME['host']}/{DB_INFORME['database']}")
            with eng_my.begin() as conn:
                conn.execute(text("TRUNCATE TABLE INFORME"))
                db_cols = [r[0] for r in conn.execute(text("SHOW COLUMNS FROM INFORME"))]
                df[df.columns.intersection(db_cols)].to_sql('INFORME', con=conn, if_exists='append', index=False)

            # 5. Postgres QGIS
            p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
            eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
            with eng_pg.connect() as conn:
                with conn.begin():
                    for _, row in df.iterrows():
                        params = {"id": str(row['ID']).strip()}
                        sets = []
                        for c_csv, c_pg in MAPEO_POSTGRES.items():
                            if c_csv in df.columns:
                                val = row[c_csv]
                                sets.append(f'"{c_pg}" = :{c_pg}')
                                params[c_pg] = None if pd.isna(val) else val
                        if sets:
                            conn.execute(text(f'UPDATE public."Pozos" SET {", ".join(sets)} WHERE "ID" = :id'), params)

            status.update(label="✅ Sincronización Finalizada", state="complete")
    except Exception as e:
        st.error(f"Error: {e}")

# --- 5. INTERFAZ ---
st.title("🖥️ MIAA Control Center")

tab_mon, tab_pg = st.tabs(["🚀 Monitor", "🔍 Postgres (QGIS)"])

with tab_mon:
    if st.button("🔄 EJECUTAR SINCRONIZACIÓN (Prioridad SCADA > 0)", type="primary"):
        ejecutar_actualizacion()

with tab_pg:
    st.subheader("Datos en QGIS")
    if st.button("Verificar P002"):
        p_pg = urllib.parse.quote_plus(DB_POSTGRES['pass'])
        eng_pg = create_engine(f"postgresql+psycopg2://{DB_POSTGRES['user']}:{p_pg}@{DB_POSTGRES['host']}:{DB_POSTGRES['port']}/{DB_POSTGRES['db']}")
        df_pg = pd.read_sql('SELECT "ID", "_Caudal", "_Presion" FROM public."Pozos" WHERE "ID" = \'P002\'', eng_pg)
        st.table(df_pg)
