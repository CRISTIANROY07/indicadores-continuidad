# ═══════════════════════════════════════════════════════════════════
# SISTEMA DE INDICADORES DE CONTINUIDAD  v8.0
# Ing. Cristian Braulio Rollano M.
# ═══════════════════════════════════════════════════════════════════
import streamlit as st
import pandas as pd
import numpy as np
import pyodbc
import tempfile, os, io, platform, subprocess, json, pickle, gzip
from datetime import datetime
import plotly.graph_objects as go
import plotly.express as px
import streamlit.components.v1 as components

st.set_page_config(page_title="Indicadores de Continuidad", layout="wide",
                   page_icon="⚡", initial_sidebar_state="expanded")

# ═══════════════════════════════════════════════════════════════════
# RUTAS Y DIRECTORIOS
# ═══════════════════════════════════════════════════════════════════
BASE_DIR  = os.path.dirname(os.path.abspath(__file__))
DATOS_DIR = os.path.join(BASE_DIR, 'datos')
REP_DIR   = os.path.join(DATOS_DIR, 'reportes')
LOG_FILE  = os.path.join(DATOS_DIR, 'log.json')   # solo admin puede leer
IDX_FILE  = os.path.join(REP_DIR, 'index.json')
for _d in [DATOS_DIR, REP_DIR]:
    os.makedirs(_d, exist_ok=True)

# ═══════════════════════════════════════════════════════════════════
# CONSTANTES
# ═══════════════════════════════════════════════════════════════════
USUARIOS   = {"crollano":"admin123","jevargas":"admin123",
               "dromero":"admin123","jcamacho":"admin123"}
ADMIN_USER = "crollano"   # único usuario que ve el panel de administración

CAUSAS_PROG = {'20','21','22','23','24'}
MAPA_ORIGEN = {'01':'SUBTRANSMISIÓN','02':'DIST. PRIMARIA','03':'DIST. SECUNDARIA',
               '10':'EXTERNO','11':'EXTERNO','12':'EXTERNO'}
MAPA_CAUSA  = {
    '20':'Ampliación o Mejoras',      '21':'Reparaciones',
    '22':'Mantenimiento Preventivo',  '23':'Poda o Derribo de Árboles',
    '24':'Programada No Clasificada',
    '30':'Descargas Atmosféricas',    '31':'Lluvia',       '32':'Viento',
    '33':'Nevada o Granizo',          '34':'Inundación',   '35':'Incendio',
    '36':'Deslizamiento de Tierra',   '37':'Caída de Árboles',
    '40':'Aves u Otros Animales',     '41':'Daño Intencional',
    '42':'Daño Accidental',           '43':'Falla Acometida Consumidores',
    '44':'Choque de Vehículos',
    '50':'Trab. Línea Viva',          '51':'Error de Operación',
    '52':'Sobre Carga',               '53':'Construcción Deficiente',
    '54':'Equipos Incorrectos',       '55':'Mala Op. Protecciones',
    '56':'Deterioro por Envejecimiento','57':'Falta de Mantenimiento',
    '58':'Líneas Reventadas',         '59':'Líneas Trenzadas',
    '60':'No Clasificadas',           '61':'No Determinadas',
}
GRUPOS_CAUSA = {
    'Interrupciones Programadas':        ['20','21','22','23','24'],
    'Cond. Climáticas / Medio Ambiente': ['30','31','32','33','34','35','36','37'],
    'Animales / Terceros':               ['40','41','42','43','44'],
    'Propias de la Red':                 ['50','51','52','53','54','55','56','57','58','59'],
    'Otras Causas':                      ['60','61'],
}
COL_GRUPO = {
    'Interrupciones Programadas':'#1976D2',
    'Cond. Climáticas / Medio Ambiente':'#00796B',
    'Animales / Terceros':'#F57C00',
    'Propias de la Red':'#C62828',
    'Otras Causas':'#7B1FA2',
}

# Límites normativos semestrales (ST10+ST11)
LIM_T_SEM_C1=6.0;  LIM_T_MES_C1=LIM_T_SEM_C1/6
LIM_F_SEM_C1=7.0;  LIM_F_MES_C1=LIM_F_SEM_C1/6
LIM_T_SEM_C2=12.0; LIM_T_MES_C2=LIM_T_SEM_C2/6
LIM_F_SEM_C2=14.0; LIM_F_MES_C2=LIM_F_SEM_C2/6

# Paleta de colores
C_AZUL="#1565C0"; C_VERDE="#00695C"; C_MORADO="#4527A0"
C_NAR="#E65100";  C_ROJO="#B71C1C"; C_AMBER="#F59E0B"
C_PROG="#1976D2"; C_FORZ="#C62828"; C_TEAL="#00796B"
C_BG="#F2F5FA";   C_GRID="rgba(150,150,150,0.2)"
C_WHITE="#FFFFFF"; C_DARK="#002855"
PALETA=["#1565C0","#C62828","#00695C","#F59E0B","#4527A0",
        "#E65100","#00796B","#AD1457","#0277BD","#558B2F"]

# ═══════════════════════════════════════════════════════════════════
# SESSION STATE
# ═══════════════════════════════════════════════════════════════════
for k,v in {'logueado':False,'usuario_actual':'','h_lim':2,'reporte_activo':None}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ═══════════════════════════════════════════════════════════════════
# LOG DE ACCESO (privado — solo ADMIN_USER puede leerlo)
# ═══════════════════════════════════════════════════════════════════
def registrar_log(usuario:str, accion:str, detalle:str=''):
    """Registra cada acceso. El archivo es ilegible para usuarios normales."""
    try:
        log = []
        if os.path.exists(LOG_FILE):
            with open(LOG_FILE,'r',encoding='utf-8') as f:
                log = json.load(f)
        log.append({
            'usuario': usuario,
            'accion':  accion,
            'detalle': detalle,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        })
        with open(LOG_FILE,'w',encoding='utf-8') as f:
            json.dump(log, f, ensure_ascii=False, indent=2)
    except Exception:
        pass

def leer_log():
    if not os.path.exists(LOG_FILE): return []
    try:
        with open(LOG_FILE,'r',encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

# ═══════════════════════════════════════════════════════════════════
# BIBLIOTECA — guardar / cargar / eliminar reportes
# Ahora guarda también las tablas crudas (ST10, ST11, Excel)
# para poder reproducir el cálculo completo en cualquier momento.
# ═══════════════════════════════════════════════════════════════════
def leer_indice():
    if not os.path.exists(IDX_FILE): return []
    try:
        with open(IDX_FILE,'r',encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return []

def guardar_reporte(nombre:str, usuario:str, datos:dict) -> bool:
    ts    = datetime.now().strftime('%Y%m%d_%H%M%S')
    fname = f"{usuario}_{ts}.pkl.gz"
    fpath = os.path.join(REP_DIR, fname)
    try:
        # Compresión gzip para reducir tamaño en disco
        with gzip.open(fpath, 'wb') as f:
            pickle.dump(datos, f, protocol=pickle.HIGHEST_PROTOCOL)
        idx = leer_indice()
        idx.append({
            'nombre':    nombre,
            'usuario':   usuario,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'archivo':   fname,
            'tablas':    datos.get('_tablas_guardadas', []),
        })
        with open(IDX_FILE,'w',encoding='utf-8') as f:
            json.dump(idx, f, ensure_ascii=False, indent=2)
        registrar_log(usuario, 'GUARDAR_REPORTE', nombre)
        return True
    except Exception as e:
        st.error(f"Error guardando reporte: {e}")
        return False

def cargar_reporte(fname:str):
    fpath = os.path.join(REP_DIR, fname)
    try:
        if fname.endswith('.gz'):
            with gzip.open(fpath,'rb') as f:
                return pickle.load(f)
        else:
            with open(fpath,'rb') as f:
                return pickle.load(f)
    except Exception as e:
        st.error(f"Error cargando reporte: {e}")
        return None

def eliminar_reporte(fname:str) -> bool:
    try:
        fpath = os.path.join(REP_DIR, fname)
        if os.path.exists(fpath): os.remove(fpath)
        idx = [r for r in leer_indice() if r['archivo'] != fname]
        with open(IDX_FILE,'w',encoding='utf-8') as f:
            json.dump(idx, f, ensure_ascii=False, indent=2)
        return True
    except Exception:
        return False

# ═══════════════════════════════════════════════════════════════════
# LOGIN
# ═══════════════════════════════════════════════════════════════════
if not st.session_state['logueado']:
    st.markdown("""<style>
    .stApp{background:linear-gradient(135deg,#001533 0%,#003080 60%,#0057B7 100%)!important}
    label[data-testid="stWidgetLabel"] p{color:#002855!important;font-weight:700!important}
    </style>""", unsafe_allow_html=True)
    _, cm, _ = st.columns([1, 1.1, 1])
    with cm:
        st.markdown("<br><br>", unsafe_allow_html=True)
        st.markdown("""<div style='background:white;border-radius:20px;padding:36px 44px 24px;
        box-shadow:0 28px 90px rgba(0,0,0,.5)'>
        <div style='text-align:center;margin-bottom:26px'>
        <div style='font-size:3.2rem'>⚡</div>
        <h2 style='color:#002855;margin:8px 0 4px;font-family:Segoe UI;font-size:1.5rem;font-weight:800'>
        Sistema de Indicadores de Continuidad</h2>
        <p style='color:#999;font-size:.82rem;margin:0'>
        Ing. Cristian Braulio Rollano M. · Solo personal autorizado</p>
        </div></div>""", unsafe_allow_html=True)
        with st.form("login_form"):
            st.markdown("<p style='color:#002855;font-weight:700;font-size:.95rem;margin:14px 0 2px'>👤 Usuario</p>",
                        unsafe_allow_html=True)
            user = st.text_input("u", placeholder="Usuario corporativo…", label_visibility="collapsed")
            st.markdown("<p style='color:#002855;font-weight:700;font-size:.95rem;margin:10px 0 2px'>🔑 Contraseña</p>",
                        unsafe_allow_html=True)
            pwd = st.text_input("p", placeholder="Contraseña…", type="password", label_visibility="collapsed")
            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button("▶  Ingresar al Sistema", use_container_width=True):
                if USUARIOS.get(user) == pwd:
                    st.session_state['logueado']       = True
                    st.session_state['usuario_actual'] = user
                    registrar_log(user, 'LOGIN')
                    st.rerun()
                else:
                    st.error("⚠️ Usuario o contraseña incorrectos.")
    st.stop()

# ═══════════════════════════════════════════════════════════════════
# ESTILOS GLOBALES (post-login)
# ═══════════════════════════════════════════════════════════════════
st.markdown(f"""<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');
.stApp{{background:linear-gradient(160deg,#E8EEF8 0%,#DDE5F2 100%);font-family:'Inter',sans-serif}}
h1,h2,h3,h4{{color:{C_DARK};font-family:'Inter',sans-serif;font-weight:700}}
.creator-badge{{background:linear-gradient(90deg,{C_DARK},{C_AZUL});color:white;padding:5px 20px;
  border-radius:30px;font-size:.86rem;font-weight:600;display:inline-block;
  box-shadow:0 4px 16px rgba(0,40,85,.3);margin-bottom:14px}}
.sec-header{{background:linear-gradient(90deg,{C_DARK} 0%,#0044AA 100%);color:white;
  padding:12px 24px;border-radius:12px;font-size:1.0rem;font-weight:700;
  margin:28px 0 18px;box-shadow:0 4px 14px rgba(0,40,85,.25);letter-spacing:.3px}}
.kpi-card{{background:white;border-radius:14px;padding:20px 22px;
  box-shadow:0 4px 20px rgba(0,40,85,.1);border-left:5px solid;transition:transform .2s}}
.kpi-card:hover{{transform:translateY(-2px);box-shadow:0 8px 28px rgba(0,40,85,.15)}}
.kpi-val{{font-size:1.9rem;font-weight:800;line-height:1;margin-bottom:4px}}
.kpi-lbl{{font-size:.76rem;color:#666;font-weight:600;text-transform:uppercase;letter-spacing:.8px}}
.kpi-sub{{font-size:.73rem;color:#999;margin-top:3px}}
.norm-box{{background:white;border-left:5px solid {C_AZUL};border-radius:12px;
  padding:16px 24px;margin-bottom:20px;box-shadow:0 2px 14px rgba(0,0,0,.07)}}
.exc-badge{{background:linear-gradient(90deg,#7B1FA2,#AB47BC);color:white;
  padding:3px 12px;border-radius:20px;font-size:.78rem;font-weight:700;
  display:inline-block;margin-left:8px}}
.exc-box{{background:linear-gradient(135deg,#F3E5F5,#EDE7F6);
  border:2px solid #CE93D8;border-radius:14px;padding:18px 22px;margin-bottom:18px}}
.warn-box{{background:#FFF8E1;border:2px solid #FFB300;border-radius:12px;padding:14px 20px;margin-bottom:16px}}
.bib-card{{background:white;border-radius:12px;padding:14px 18px;margin-bottom:8px;
  box-shadow:0 2px 8px rgba(0,40,85,.08);border-left:4px solid {C_AZUL};
  display:flex;align-items:center;gap:10px}}
button[data-baseweb="tab"]{{font-family:'Inter',sans-serif!important;
  font-size:.9rem!important;font-weight:600!important;color:{C_DARK}!important}}
button[data-baseweb="tab"][aria-selected="true"]{{
  color:{C_AZUL}!important;border-bottom:3px solid {C_AZUL}!important}}
.stDataFrame{{border-radius:12px;overflow:hidden;box-shadow:0 2px 12px rgba(0,0,0,.08)}}
[data-testid="stMetricValue"]{{font-size:1.4rem!important;font-weight:800!important}}
[data-testid="stMetricLabel"]{{font-size:.78rem!important;color:#555!important}}
.stDownloadButton>button,.stButton>button{{
  background:linear-gradient(90deg,{C_DARK},{C_AZUL})!important;
  color:white!important;border:none!important;border-radius:10px!important;
  font-weight:700!important;padding:8px 18px!important;
  box-shadow:0 3px 12px rgba(0,40,85,.28)!important}}
.stNumberInput label p,.stSelectbox label p{{
  color:{C_DARK}!important;font-weight:600!important;font-size:.86rem!important}}
</style>""", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# CABECERA
# ═══════════════════════════════════════════════════════════════════
cH1, cH2 = st.columns([5, 1])
with cH1:
    st.title("⚡ Sistema de Indicadores de Continuidad")
    st.markdown("<div class='creator-badge'>👨‍💻 Ing. Cristian Braulio Rollano M.</div>",
                unsafe_allow_html=True)
with cH2:
    st.markdown("<br>", unsafe_allow_html=True)
    st.info(f"👤 **{st.session_state['usuario_actual']}**")
    if st.button("⏻  Salir"):
        registrar_log(st.session_state['usuario_actual'], 'LOGOUT')
        st.session_state['logueado'] = False
        st.rerun()

# ═══════════════════════════════════════════════════════════════════
# SIDEBAR
# ═══════════════════════════════════════════════════════════════════
st.sidebar.markdown(f"""<div style='background:linear-gradient(135deg,{C_DARK},{C_AZUL});
  border-radius:12px;padding:14px 18px;margin-bottom:16px;box-shadow:0 4px 16px rgba(0,40,85,.3)'>
  <p style='color:white;font-size:1.0rem;font-weight:700;margin:0'>⚙️ Parámetros del Sistema</p>
</div>""", unsafe_allow_html=True)

st.sidebar.markdown(f"""<div style='background:white;border-radius:12px;padding:14px;
  box-shadow:0 2px 12px rgba(0,0,0,.08);margin-bottom:14px'>
  <p style='font-size:.76rem;font-weight:700;color:{C_DARK};margin:0 0 10px;text-transform:uppercase'>
  📋 Límites Normativos Globales</p>
  <table style='width:100%;border-collapse:collapse;font-size:.80rem'>
  <tr><th style='background:{C_DARK};color:#fff;padding:6px 4px;text-align:left'>Indicador</th>
      <th style='background:{C_DARK};color:#fff;padding:6px 0;text-align:center'>Cal. 1</th>
      <th style='background:{C_DARK};color:#fff;padding:6px 0;text-align:center'>Cal. 2</th></tr>
  <tr style='background:#F0F5FF'>
    <td style='padding:7px 4px;font-weight:600'>⏱️ Tiempo</td>
    <td style='text-align:center;font-weight:700;color:{C_AZUL}'>6 h</td>
    <td style='text-align:center;font-weight:700;color:{C_VERDE}'>12 h</td></tr>
  <tr>
    <td style='padding:7px 4px;font-weight:600'>🔢 Frecuencia</td>
    <td style='text-align:center;font-weight:700;color:{C_AZUL}'>7</td>
    <td style='text-align:center;font-weight:700;color:{C_VERDE}'>14</td></tr>
  <tr style='background:#F6F9FF'>
    <td colspan='3' style='padding:6px 4px;color:#777;font-size:.75rem;font-style:italic'>
    Límite mensual = Semestral ÷ 6 · Solo ≥ 3 min</td></tr>
  </table></div>""", unsafe_allow_html=True)

with st.sidebar.form("params_form"):
    cs_1 = st.number_input("Consumidores Calidad 1", value=12537.667, format="%.3f")
    cs_2 = st.number_input("Consumidores Calidad 2", value=1392.833,  format="%.3f")
    st.form_submit_button("✔  Actualizar Parámetros", use_container_width=True)

# ═══════════════════════════════════════════════════════════════════
# LEER MDB — cacheado por bytes del archivo
# ═══════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner=False)
def leer_mdb(file_bytes: bytes, tabla: str):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mdb') as tmp:
        tmp.write(file_bytes)
        path = tmp.name
    try:
        if platform.system() == "Windows":
            conn = pyodbc.connect(
                r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + path + ';')
            df = pd.read_sql(f'SELECT * FROM {tabla}', conn)
            conn.close()
            return df
        else:
            res = subprocess.run(['mdb-export', path, tabla],
                                 capture_output=True, text=True, timeout=60)
            if res.returncode != 0:
                st.error(f"Error leyendo **{tabla}**: {res.stderr.strip()}")
                return None
            if not res.stdout.strip():
                st.error(f"Tabla **{tabla}** vacía o no encontrada.")
                return None
            return pd.read_csv(io.StringIO(res.stdout), low_memory=False)
    except Exception as e:
        st.error(f"Error {tabla}: {e}")
        return None
    finally:
        if os.path.exists(path):
            os.remove(path)

# ═══════════════════════════════════════════════════════════════════
# HELPERS GENERALES
# ═══════════════════════════════════════════════════════════════════
def _num_str(series) -> pd.Series:
    """Convierte NUMERO (int/float/str) a string limpio: 12345.0 → '12345'."""
    return pd.to_numeric(series, errors='coerce').fillna(0).astype(int).astype(str)

def _parse_fecha(fecha_col, hora_col):
    fechas = pd.to_datetime(fecha_col, errors='coerce').dt.strftime('%Y-%m-%d')
    horas  = hora_col.astype(str).str.split().str[-1].copy()
    mask   = ~horas.str.contains(':', na=False)
    nums   = pd.to_numeric(horas[mask], errors='coerce').fillna(0).astype(int)
    horas.loc[mask] = (
        (nums // 10000).astype(str).str.zfill(2) + ':' +
        ((nums % 10000) // 100).astype(str).str.zfill(2) + ':' +
        (nums % 100).astype(str).str.zfill(2)
    )
    return pd.to_datetime(fechas + ' ' + horas, errors='coerce')

def get_grupo(cod):
    c = str(cod).zfill(2)
    for g, ls in GRUPOS_CAUSA.items():
        if c in ls: return g
    return 'Otras Causas'

def to_excel(df: pd.DataFrame, sheet='Datos') -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name=sheet[:31])
    return buf.getvalue()

def to_excel_multi(dfs: dict) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        for s, d in dfs.items():
            d.to_excel(w, index=False, sheet_name=s[:31])
    return buf.getvalue()

# ═══════════════════════════════════════════════════════════════════
# PROCESAR DATOS GLOBALES (ST10 + ST11) — cacheado
# ═══════════════════════════════════════════════════════════════════
@st.cache_data(show_spinner=False)
def procesar_datos(bytes_10: bytes, bytes_11: bytes, nums_excluir: tuple = ()):
    df10_raw = leer_mdb(bytes_10, "ST10")
    df11_raw = leer_mdb(bytes_11, "ST11")
    if df10_raw is None or df11_raw is None:
        return [None] * 7

    # Normalizar NUMERO (float-safe: 12345.0 → '12345')
    df10_raw = df10_raw.copy()
    df11_raw = df11_raw.copy()
    df10_raw['NUMERO'] = _num_str(df10_raw['NUMERO'])
    df11_raw['NUMERO'] = _num_str(df11_raw['NUMERO'])

    # Parsear fechas/horas
    df10_raw['START'] = _parse_fecha(df10_raw['FECHA_I'], df10_raw['HORA_I'])
    df11_raw['END']   = _parse_fecha(df11_raw['FECHA_R'], df11_raw['HORA_R'])

    # Unir ST10 con la última reposición de ST11
    df11_last = df11_raw.groupby('NUMERO', sort=False)['END'].max().reset_index()
    df10_full = df10_raw.merge(df11_last, on='NUMERO', how='inner')

    # Calcular duración
    df10_full['DURACION_H'] = (
        (df10_full['END'] - df10_full['START']).dt.total_seconds() / 3600.0)
    df10_full = df10_full[df10_full['DURACION_H'] >= 0].copy()

    # Etiquetas de origen y causa
    df10_full['ORIGEN_LABEL'] = (
        df10_full['COD_ORIGEN'].astype(str).str.zfill(2).map(MAPA_ORIGEN).fillna('OTROS'))
    df10_full['CAUSA_LABEL'] = (
        df10_full['COD_CAUSA'].astype(str).str.zfill(2).map(MAPA_CAUSA).fillna('Otros'))
    df10_full['GRUPO_CAUSA'] = df10_full['COD_CAUSA'].apply(get_grupo)
    df10_full['TIPO_LABEL']  = np.where(
        df10_full['COD_CAUSA'].astype(str).str.strip().isin(CAUSAS_PROG),
        'PROGRAMADA', 'FORZADA')

    # Separar causa invocada
    exc_set = set(nums_excluir)
    if exc_set:
        mask_exc     = df10_full['NUMERO'].isin(exc_set)
        df_causa_inv = df10_full[mask_exc].copy()
        df10         = df10_full[~mask_exc].copy()
    else:
        df_causa_inv = pd.DataFrame()
        df10         = df10_full.copy()

    # Interrupciones < 3 min (informativo)
    MIN3 = 3.0 / 60.0
    df_menor3 = df10[
        (df10['ORIGEN_LABEL'] != 'EXTERNO') &
        (df10['DURACION_H'] < MIN3) &
        (df10['DURACION_H'] >= 0)
    ].copy()

    # SIR: sin externo, >= 3 min
    df_sir_10 = df10[
        (df10['ORIGEN_LABEL'] != 'EXTERNO') &
        (df10['DURACION_H'] >= MIN3)
    ].copy()
    df_sir_10['MES'] = df_sir_10['START'].dt.to_period('M').astype(str)

    # Unir SIR con ST11 para obtener tiempos ponderados
    cols_join = ['NUMERO','START','ORIGEN_LABEL','CAUSA_LABEL',
                 'GRUPO_CAUSA','COD_CAUSA','TIPO_LABEL','MES']
    df_sir_11 = df11_raw.merge(df_sir_10[cols_join], on='NUMERO', how='inner')
    df_sir_11['DURACION_PASO'] = (
        (df_sir_11['END'] - df_sir_11['START']).dt.total_seconds() / 3600.0)
    df_sir_11 = df_sir_11[df_sir_11['DURACION_PASO'] >= 0].copy()
    for cal, col_r in [(1, 'CONS_BT_R_1'), (2, 'CONS_BT_R_2')]:
        df_sir_11[f'T_POND_{cal}'] = (
            df_sir_11[col_r] * df_sir_11['DURACION_PASO']
            if col_r in df_sir_11.columns else 0.0)

    # Conteo para verificación ST10 vs ST11
    n10 = df10_raw['NUMERO'].nunique()
    n11 = df11_raw['NUMERO'].nunique()

    return df10, df_sir_10, df_sir_11, df10_full, df_causa_inv, df_menor3, (n10, n11)

# ═══════════════════════════════════════════════════════════════════
# TABLA SIR GLOBAL
# ═══════════════════════════════════════════════════════════════════
def armar_tabla_sir(df_sir_10, df_sir_11, cs_val, cal):
    FILAS = ['SUBTRANSMISIÓN', 'DIST. PRIMARIA', 'DIST. SECUNDARIA']
    col_f = f'CONS_BT_{cal}'
    col_t = f'T_POND_{cal}'

    def _piv(df, col, group):
        if col not in df.columns:
            return pd.DataFrame(0.0, index=FILAS, columns=['PROGRAMADA', 'FORZADA'])
        p = (df.groupby(group)[col].sum() / cs_val).unstack(fill_value=0)
        for c in ['PROGRAMADA', 'FORZADA']:
            if c not in p.columns: p[c] = 0.0
        return p.reindex(FILAS, fill_value=0)

    p_f = _piv(df_sir_10, col_f, ['ORIGEN_LABEL', 'TIPO_LABEL'])
    p_t = _piv(df_sir_11, col_t, ['ORIGEN_LABEL', 'TIPO_LABEL'])

    out = pd.DataFrame(index=FILAS)
    out['(F) PROGRAMADA'] = p_f['PROGRAMADA']
    out['(F) FORZADA']    = p_f['FORZADA']
    out['(F) TOTAL']      = out['(F) PROGRAMADA'] + out['(F) FORZADA']
    out['(T) PROGRAMADA'] = p_t['PROGRAMADA']
    out['(T) FORZADA']    = p_t['FORZADA']
    out['(T) TOTAL']      = out['(T) PROGRAMADA'] + out['(T) FORZADA']
    # TOTAL DIST. = suma las 3 filas (incluyendo SUBTRANSMISIÓN)
    out.loc['TOTAL DIST.'] = out.loc[['SUBTRANSMISIÓN', 'DIST. PRIMARIA', 'DIST. SECUNDARIA']].sum()
    return out

# ═══════════════════════════════════════════════════════════════════
# REPORTE HTML
# ═══════════════════════════════════════════════════════════════════
def generar_html(res_c1, res_c2):
    return f"""<html><head><meta charset="UTF-8"><style>
    body{{font-family:'Segoe UI',sans-serif;padding:40px;color:#333;background:#f9f9f9}}
    h1,h2{{color:#002855}} h1{{border-bottom:3px solid #1565C0;padding-bottom:10px}}
    table{{width:100%;border-collapse:collapse;margin-bottom:30px;font-size:13px;
           box-shadow:0 2px 8px rgba(0,0,0,.1)}}
    th{{background:#002855;color:white;padding:10px 14px;text-align:center;font-weight:600}}
    td{{border:1px solid #ddd;padding:8px 12px;text-align:center}}
    tr:nth-child(even){{background:#F0F5FF}}
    tr:last-child{{background:#E8F5E9;font-weight:700}}
    .footer{{margin-top:50px;font-size:11px;color:#aaa;text-align:center;
             border-top:1px solid #eee;padding-top:16px}}
    </style></head><body>
    <h1>⚡ Reporte de Indicadores de Continuidad</h1>
    <p style='color:#555'>Ing. Cristian Braulio Rollano M.</p>
    <h2>🔹 Calidad 1</h2>{res_c1.round(4).to_html()}
    <h2 style='margin-top:30px'>🔹 Calidad 2</h2>{res_c2.round(4).to_html()}
    <div class="footer">Ing. Cristian Braulio Rollano M. — Sistema de Indicadores de Continuidad</div>
    <script>window.onload=()=>window.print()</script>
    </body></html>""".encode('utf-8')

# ═══════════════════════════════════════════════════════════════════
# HELPERS GRÁFICAS
# ═══════════════════════════════════════════════════════════════════
_LAY = dict(plot_bgcolor=C_WHITE, paper_bgcolor=C_BG,
            font=dict(family='Inter,Segoe UI,sans-serif', color='#333'))

def _fig(**kw) -> go.Figure:
    f = go.Figure()
    f.update_layout(**{**_LAY, **kw})
    return f

def _tit(txt, sub=""):
    t = f"<b>{txt}</b>" + (
        f"<br><sup style='color:#666;font-size:11px'>{sub}</sup>" if sub else "")
    return dict(text=t, font=dict(size=15, color=C_DARK), x=0.01, xanchor='left')

def _mes_fmt(m):
    try:
        dt  = pd.Period(m, 'M').to_timestamp()
        nom = ['Ene','Feb','Mar','Abr','May','Jun',
               'Jul','Ago','Sep','Oct','Nov','Dic'][dt.month - 1]
        return f"{nom}<br>{str(dt.year)[2:]}"
    except Exception:
        return m

# ═══════════════════════════════════════════════════════════════════
# KPI CARDS
# ═══════════════════════════════════════════════════════════════════
def mostrar_kpis(df10, df_causa_inv, df_menor3):
    total = len(df10)
    prog  = int((df10['TIPO_LABEL'] == 'PROGRAMADA').sum())
    forz  = total - prog
    ext   = int((df10['ORIGEN_LABEL'] == 'EXTERNO').sum())
    excl  = len(df_causa_inv) if df_causa_inv is not None and not df_causa_inv.empty else 0
    men3  = len(df_menor3) if df_menor3 is not None else 0
    dur_m = df10['DURACION_H'].mean() if total > 0 else 0
    en_sir = total - ext - excl

    for row in [
        [(str(total), "Total Interrupciones", "Registros ST10",       C_AZUL),
         (str(forz),  "Forzadas",             "Sin mant. programado", C_ROJO),
         (str(prog),  "Programadas",           "Mant. programado",    C_VERDE),
         (str(ext),   "Origen Externo",        "Cód. 10, 11, 12",     C_MORADO)],
        [(f"{dur_m:.2f} h", "Duración Promedio", "Por interrupción",    C_NAR),
         (str(men3),        "< 3 Minutos",       "Excluidas del SIR",  "#78909C"),
         (str(excl),        "Causa Invocada",    "Excepciones excluidas","#7B1FA2"),
         (str(en_sir),      "Procesadas SIR",    "Sin externo/excluidas",C_TEAL)],
    ]:
        cols = st.columns(4)
        for col, (val, lbl, sub, color) in zip(cols, row):
            col.markdown(f"""<div class='kpi-card' style='border-left-color:{color}'>
              <div class='kpi-val' style='color:{color}'>{val}</div>
              <div class='kpi-lbl'>{lbl}</div><div class='kpi-sub'>{sub}</div>
              </div>""", unsafe_allow_html=True)
        st.markdown("<div style='height:10px'></div>", unsafe_allow_html=True)

# ═══════════════════════════════════════════════════════════════════
# GRÁFICA BARRAS MENSUALES (con hover % excedido)
# ═══════════════════════════════════════════════════════════════════
def _bar_mes(df, titulo, y_label, lim_mes, color_ok, unidad):
    if df.empty or df['VALOR'].sum() == 0:
        return None, 0
    meses   = df['MES'].tolist()
    valores = df['VALOR'].tolist()
    supera  = df['supera'].tolist()
    n_inc   = sum(supera)
    colores = [C_ROJO if s else color_ok for s in supera]
    pct_lim = [v / lim_mes * 100 if lim_mes > 0 else 0 for v in valores]
    exceso  = [max(0, v - lim_mes) / lim_mes * 100 if lim_mes > 0 else 0 for v in valores]
    fig = _fig(
        title=_tit(titulo), height=520,
        margin=dict(t=80, b=170, l=80, r=36),
        xaxis=dict(tickmode='array', tickvals=list(range(len(meses))),
                   ticktext=[_mes_fmt(m) for m in meses], tickangle=0,
                   tickfont=dict(size=11, color='#333'),
                   showgrid=False, linecolor='#ccd0da', fixedrange=True),
        yaxis=dict(title=dict(text=y_label, font=dict(size=12)),
                   showgrid=True, gridcolor=C_GRID, tickfont=dict(size=11),
                   zeroline=True, zerolinecolor='#ccd0da'),
        bargap=0.30, showlegend=False)
    custom = [[m, p, f"⚠️ Excede en {e:.1f}%" if s else "✅ Dentro del límite"]
              for m, p, e, s in zip(meses, pct_lim, exceso, supera)]
    fig.add_trace(go.Bar(
        x=list(range(len(meses))), y=valores,
        marker=dict(color=colores, line=dict(color='white', width=1.5), opacity=0.93),
        customdata=custom,
        hovertemplate=("<b>%{customdata[0]}</b><br>" + y_label +
                       ": <b>%{y:.4f}</b> " + unidad +
                       "<br>% del límite: <b>%{customdata[1]:.1f}%</b>"
                       "<br><b>%{customdata[2]}</b><extra></extra>"),
        text=[f"{v:.3f}" for v in valores],
        textposition='outside', textfont=dict(size=9, color='#555')))
    fig.add_hline(
        y=lim_mes, line=dict(color=C_AMBER, width=2.5, dash='dot'),
        annotation=dict(text=f"<b>Límite: {lim_mes:.3f} {unidad}</b>",
                        font=dict(size=11, color=C_AMBER), bgcolor='white',
                        bordercolor=C_AMBER, borderwidth=1, borderpad=4,
                        xanchor='right', x=1.0))
    fig.add_annotation(
        xref='paper', yref='paper', x=0.5, y=-0.38,
        xanchor='center', yanchor='top',
        text=(f"<span style='color:{color_ok};font-size:14px'>█</span>  Dentro del límite"
              f"&nbsp;&nbsp;&nbsp;<span style='color:{C_ROJO};font-size:14px'>█</span>"
              f"  Supera el límite"
              f"&nbsp;&nbsp;&nbsp;<span style='color:{C_AMBER}'>· · ·</span>"
              f"  Límite ({lim_mes:.3f} {unidad})"),
        showarrow=False, font=dict(size=11, color='#444'),
        bgcolor='rgba(255,255,255,.95)',
        bordercolor='rgba(180,180,180,.5)', borderwidth=1, borderpad=10)
    return fig, n_inc

# ═══════════════════════════════════════════════════════════════════
# GRÁFICAS INCIDENCIA MENSUAL
# ═══════════════════════════════════════════════════════════════════
def graficar_incidencia_mensual(df_sir_10, df_sir_11, _cs1, _cs2):
    def _t_mes(col_t, cs, lim):
        if col_t not in df_sir_11.columns: return pd.DataFrame()
        df = (df_sir_11.groupby('MES')[col_t].sum() / cs).reset_index()
        df.columns = ['MES', 'VALOR']
        df['supera'] = df['VALOR'] > lim
        return df.sort_values('MES')

    def _f_mes(col_f, cs, lim):
        if col_f in df_sir_10.columns:
            df = (df_sir_10.groupby('MES')[col_f].sum() / cs).reset_index()
            df.columns = ['MES', 'VALOR']
        else:
            df = df_sir_10.groupby('MES').size().reset_index(name='VALOR')
            df['VALOR'] = df['VALOR'] / cs
        df['supera'] = df['VALOR'] > lim
        return df.sort_values('MES')

    # Tiempo mensual
    st.markdown("<div class='sec-header'>⏱️ Tiempo de Interrupción Mensual (h / usuario)</div>",
                unsafe_allow_html=True)
    df_t1 = _t_mes('T_POND_1', _cs1, LIM_T_MES_C1)
    df_t2 = _t_mes('T_POND_2', _cs2, LIM_T_MES_C2)
    ct1, ct2 = st.columns(2)
    for col, df, tt, lim, c_ok in [
        (ct1, df_t1, "Tiempo Mensual — Calidad 1", LIM_T_MES_C1, C_AZUL),
        (ct2, df_t2, "Tiempo Mensual — Calidad 2", LIM_T_MES_C2, C_VERDE),
    ]:
        with col:
            fig, n = _bar_mes(df, tt, "h / usuario", lim, c_ok, "h")
            if fig:
                st.plotly_chart(fig, use_container_width=True)
                (st.error if n > 0 else st.success)(
                    f"⚠️ {n} mes(es) superan el límite" if n > 0 else "✅ Todos dentro del límite")
            else:
                st.info("Sin datos")

    # Frecuencia mensual
    st.markdown("<div class='sec-header'>📊 Frecuencia de Interrupción Mensual (eventos / usuario)</div>",
                unsafe_allow_html=True)
    df_f1 = _f_mes('CONS_BT_1', _cs1, LIM_F_MES_C1)
    df_f2 = _f_mes('CONS_BT_2', _cs2, LIM_F_MES_C2)
    cf1, cf2 = st.columns(2)
    for col, df, tt, lim, c_ok in [
        (cf1, df_f1, "Frecuencia Mensual — Calidad 1", LIM_F_MES_C1, C_MORADO),
        (cf2, df_f2, "Frecuencia Mensual — Calidad 2", LIM_F_MES_C2, C_NAR),
    ]:
        with col:
            fig, n = _bar_mes(df, tt, "eventos / usuario", lim, c_ok, "eventos")
            if fig:
                st.plotly_chart(fig, use_container_width=True)
                (st.error if n > 0 else st.success)(
                    f"⚠️ {n} mes(es) superan el límite" if n > 0 else "✅ Todos dentro del límite")
            else:
                st.info("Sin datos")

    # Resumen semestral
    st.markdown("<div class='sec-header'>📈 Resumen Semestral Acumulado vs Límite Normativo</div>",
                unsafe_allow_html=True)

    def _resumen(at, af, lt, lf, titulo, ct, cf):
        cats  = ['Tiempo de Interrupción', 'Frecuencia de Interrupción']
        acums = [at, af]; lims = [lt, lf]; unids = ['h', 'eventos']
        cfin  = [C_ROJO if a > l else c for a, l, c in zip(acums, lims, [ct, cf])]
        pcts  = [(a / l * 100) if l > 0 else 0 for a, l in zip(acums, lims)]
        fig   = _fig(
            title=_tit(titulo, "Acumulado semestral vs límite normativo"),
            height=440, margin=dict(t=85, b=90, l=70, r=36),
            xaxis=dict(tickfont=dict(size=13, color=C_DARK), showgrid=False),
            yaxis=dict(title="Valor acumulado", showgrid=True,
                       gridcolor=C_GRID, tickfont=dict(size=11)),
            showlegend=True, bargap=0.44,
            legend=dict(orientation='h', x=0.5, xanchor='center', y=-0.20,
                        font=dict(size=12), bgcolor='rgba(255,255,255,.9)',
                        bordercolor='#ddd', borderwidth=1))
        for i, (cat, acum, lim, col, unid, pct) in enumerate(
                zip(cats, acums, lims, cfin, unids, pcts)):
            fig.add_trace(go.Bar(
                x=[cat], y=[acum],
                marker=dict(color=col, line=dict(color='white', width=2), opacity=0.91),
                text=[f"<b>{acum:.3f} {unid}</b><br>({pct:.0f}% del límite)"],
                textposition='outside', textfont=dict(size=11, color='#333'),
                width=0.46,
                hovertemplate=(f"<b>{cat}</b><br>{acum:.4f} {unid}"
                               f"<br>Límite: {lim}<br>{pct:.1f}%<extra></extra>"),
                showlegend=False))
            fig.add_shape(type='line', x0=i-0.27, x1=i+0.27, y0=lim, y1=lim,
                          line=dict(color=C_AMBER, width=3))
            fig.add_annotation(
                x=cat, y=lim, yshift=13,
                text=f"Límite: <b>{lim}</b> {unid} {'✅' if acum <= lim else '⚠️'}",
                showarrow=False,
                font=dict(size=10, color=C_AMBER if acum <= lim else C_ROJO),
                bgcolor='rgba(255,255,255,.9)', bordercolor=C_AMBER,
                borderwidth=1, borderpad=4)
        fig.add_trace(go.Scatter(x=[None], y=[None], mode='lines',
            line=dict(color=C_AMBER, width=3), name='Límite normativo semestral'))
        return fig

    acum_t1 = df_t1['VALOR'].sum() if not df_t1.empty else 0.0
    acum_t2 = df_t2['VALOR'].sum() if not df_t2.empty else 0.0
    acum_f1 = df_f1['VALOR'].sum() if not df_f1.empty else 0.0
    acum_f2 = df_f2['VALOR'].sum() if not df_f2.empty else 0.0
    cR1, cR2 = st.columns(2)
    with cR1:
        st.plotly_chart(
            _resumen(acum_t1, acum_f1, LIM_T_SEM_C1, LIM_F_SEM_C1,
                     "Resumen Semestral — Calidad 1", C_AZUL, C_MORADO),
            use_container_width=True)
        mA, mB = st.columns(2)
        mA.metric("Tiempo acumulado",    f"{acum_t1:.3f} h",
                  delta=f"{acum_t1-LIM_T_SEM_C1:+.3f}", delta_color="inverse")
        mB.metric("Frecuencia acumulada", f"{acum_f1:.3f}",
                  delta=f"{acum_f1-LIM_F_SEM_C1:+.3f}", delta_color="inverse")
    with cR2:
        st.plotly_chart(
            _resumen(acum_t2, acum_f2, LIM_T_SEM_C2, LIM_F_SEM_C2,
                     "Resumen Semestral — Calidad 2", C_VERDE, C_NAR),
            use_container_width=True)
        mC, mD = st.columns(2)
        mC.metric("Tiempo acumulado",    f"{acum_t2:.3f} h",
                  delta=f"{acum_t2-LIM_T_SEM_C2:+.3f}", delta_color="inverse")
        mD.metric("Frecuencia acumulada", f"{acum_f2:.3f}",
                  delta=f"{acum_f2-LIM_F_SEM_C2:+.3f}", delta_color="inverse")

# ═══════════════════════════════════════════════════════════════════
# GRÁFICA IMPACTO POR ORIGEN
# ═══════════════════════════════════════════════════════════════════
def graficar_impacto_origen(df_sir_10, df_sir_11):
    def _barra_h(col_t, titulo, cs):
        if col_t not in df_sir_11.columns or df_sir_11[col_t].sum() == 0:
            return None
        agg  = df_sir_11.groupby(['ORIGEN_LABEL','TIPO_LABEL'])[col_t].sum().reset_index()
        agg['V'] = agg[col_t] / cs
        ors  = sorted(agg['ORIGEN_LABEL'].unique())
        prog = agg[agg['TIPO_LABEL']=='PROGRAMADA'].set_index('ORIGEN_LABEL')['V'].reindex(ors, fill_value=0)
        forz = agg[agg['TIPO_LABEL']=='FORZADA'   ].set_index('ORIGEN_LABEL')['V'].reindex(ors, fill_value=0)
        tot  = prog + forz
        fig  = _fig(
            barmode='stack', title=_tit(titulo, "h/usuario acumulado"),
            height=max(350, len(ors)*90+180),
            margin=dict(t=80, b=90, l=20, r=150),
            xaxis=dict(title="Horas / usuario", showgrid=True, gridcolor=C_GRID,
                       tickfont=dict(size=11)),
            yaxis=dict(tickfont=dict(size=12, color=C_DARK), showgrid=False, automargin=True),
            legend=dict(orientation='h', x=0.5, xanchor='center', y=-0.25,
                        font=dict(size=12), bgcolor='rgba(255,255,255,.9)',
                        bordercolor='#ddd', borderwidth=1))
        fig.add_trace(go.Bar(
            name='Programada', y=ors, x=prog.values, orientation='h',
            marker=dict(color=C_PROG, line=dict(color='white', width=1.5)),
            text=[f"{v:.3f}" if v > 0.001 else "" for v in prog.values],
            textposition='inside', insidetextanchor='middle',
            textfont=dict(color='white', size=11),
            hovertemplate="<b>%{y}</b><br>Programada: %{x:.4f} h<extra></extra>"))
        fig.add_trace(go.Bar(
            name='Forzada', y=ors, x=forz.values, orientation='h',
            marker=dict(color=C_FORZ, line=dict(color='white', width=1.5)),
            text=[f"{v:.3f}" if v > 0.001 else "" for v in forz.values],
            textposition='inside', insidetextanchor='middle',
            textfont=dict(color='white', size=11),
            hovertemplate="<b>%{y}</b><br>Forzada: %{x:.4f} h<extra></extra>"))
        for org, t in zip(ors, tot.values):
            if t > 0:
                fig.add_annotation(
                    x=t, y=org, text=f"<b>{t:.3f}</b>",
                    showarrow=False, xanchor='left', yanchor='middle',
                    xshift=8, font=dict(size=11, color='#222'))
        return fig

    def _donut(col_t, titulo, cs):
        if col_t not in df_sir_11.columns or df_sir_11[col_t].sum() == 0:
            return None
        agg     = df_sir_11.groupby('TIPO_LABEL')[col_t].sum().reset_index()
        tv      = agg[col_t].sum() / cs
        colores = [C_PROG if t=='PROGRAMADA' else C_FORZ for t in agg['TIPO_LABEL']]
        fig = _fig(title=_tit(titulo), height=380,
                   margin=dict(t=75, b=70, l=20, r=20), showlegend=True,
                   legend=dict(orientation='h', x=0.5, xanchor='center',
                               y=-0.18, font=dict(size=12)))
        fig.add_trace(go.Pie(
            labels=agg['TIPO_LABEL'], values=agg[col_t], hole=0.52,
            marker=dict(colors=colores, line=dict(color='white', width=3)),
            textinfo='label+percent', textfont=dict(size=12),
            pull=[0.05 if t=='FORZADA' else 0 for t in agg['TIPO_LABEL']],
            hovertemplate="<b>%{label}</b><br>%{value:.3f} h·cons<br>%{percent}<extra></extra>"))
        fig.add_annotation(
            text=f"<b>{tv:.2f}</b><br><span style='font-size:10px'>h/usuario</span>",
            x=0.5, y=0.5, showarrow=False, font=dict(size=14, color=C_DARK))
        return fig

    c1, c2 = st.columns(2)
    with c1:
        f = _barra_h('T_POND_1', "Impacto por Origen — Calidad 1", cs_1)
        if f: st.plotly_chart(f, use_container_width=True)
        else: st.info("Sin datos Cal.1")
    with c2:
        f = _barra_h('T_POND_2', "Impacto por Origen — Calidad 2", cs_2)
        if f: st.plotly_chart(f, use_container_width=True)
        else: st.info("Sin datos Cal.2")

    st.markdown("<div class='sec-header'>📌 Distribución: Programadas vs Forzadas</div>",
                unsafe_allow_html=True)
    c3, c4 = st.columns(2)
    with c3:
        f = _donut('T_POND_1', "Distribución Tiempo — Cal. 1", cs_1)
        if f: st.plotly_chart(f, use_container_width=True)
    with c4:
        f = _donut('T_POND_2', "Distribución Tiempo — Cal. 2", cs_2)
        if f: st.plotly_chart(f, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════
# GRÁFICAS POR ORIGEN Y CAUSA
# ═══════════════════════════════════════════════════════════════════
def graficar_origen_causa(df10, df_sir_10, df_sir_11, _cs1, _cs2):
    st.markdown("<div class='sec-header'>🥧 Distribución por Origen y Tipo</div>",
                unsafe_allow_html=True)
    c1, c2 = st.columns(2)
    with c1:
        agg = (df10.groupby('ORIGEN_LABEL').size()
               .reset_index(name='N').sort_values('N', ascending=False))
        fig = _fig(title=_tit("Distribución por Origen", "% del total"),
                   height=400, margin=dict(t=75, b=20, l=20, r=140),
                   showlegend=True,
                   legend=dict(orientation='v', x=1.02, y=0.5, font=dict(size=11)))
        fig.add_trace(go.Pie(
            labels=agg['ORIGEN_LABEL'], values=agg['N'], hole=0.44,
            marker=dict(colors=PALETA, line=dict(color='white', width=2)),
            textinfo='label+percent', textfont=dict(size=11),
            pull=[0.03]*len(agg),
            hovertemplate="<b>%{label}</b><br>%{value}<br>%{percent}<extra></extra>"))
        fig.add_annotation(
            text=f"<b>{len(df10)}</b><br><span style='font-size:10px'>total</span>",
            x=0.5, y=0.5, showarrow=False, font=dict(size=15, color=C_DARK))
        st.plotly_chart(fig, use_container_width=True)
    with c2:
        agg2 = df10.groupby('TIPO_LABEL').size().reset_index(name='N')
        ct   = [C_PROG if t=='PROGRAMADA' else C_FORZ for t in agg2['TIPO_LABEL']]
        fig2 = _fig(title=_tit("Programadas vs Forzadas", "% sobre total"),
                    height=400, margin=dict(t=75, b=20, l=20, r=20),
                    showlegend=True,
                    legend=dict(orientation='h', x=0.5, xanchor='center',
                                y=-0.12, font=dict(size=12)))
        fig2.add_trace(go.Pie(
            labels=agg2['TIPO_LABEL'], values=agg2['N'], hole=0.44,
            marker=dict(colors=ct, line=dict(color='white', width=2)),
            textinfo='label+percent', textfont=dict(size=12),
            pull=[0.05]*len(agg2),
            hovertemplate="<b>%{label}</b><br>%{value}<br>%{percent}<extra></extra>"))
        st.plotly_chart(fig2, use_container_width=True)

    st.markdown("<div class='sec-header'>📊 Cantidad de Interrupciones por Causa Detallada</div>",
                unsafe_allow_html=True)
    df_tmp   = df10.copy()
    agg_c    = df_tmp.groupby(['GRUPO_CAUSA','CAUSA_LABEL','TIPO_LABEL']).size().reset_index(name='N')

    for grupo in GRUPOS_CAUSA:
        sub = agg_c[agg_c['GRUPO_CAUSA'] == grupo]
        if sub.empty: continue
        causas = (sub.groupby('CAUSA_LABEL')['N'].sum()
                  .sort_values(ascending=True).index.tolist())
        prog_c = (sub[sub['TIPO_LABEL']=='PROGRAMADA']
                  .set_index('CAUSA_LABEL')['N'].reindex(causas, fill_value=0))
        forz_c = (sub[sub['TIPO_LABEL']=='FORZADA']
                  .set_index('CAUSA_LABEL')['N'].reindex(causas, fill_value=0))
        tot_c  = prog_c + forz_c
        col_g  = COL_GRUPO.get(grupo, C_AZUL)
        fig_g  = _fig(
            barmode='stack', title=_tit(f"📂 {grupo}"),
            height=max(220, len(causas)*50+140),
            margin=dict(t=70, b=50, l=20, r=160),
            xaxis=dict(title="Cantidad", showgrid=True, gridcolor=C_GRID,
                       tickfont=dict(size=11)),
            yaxis=dict(tickfont=dict(size=11, color=C_DARK), showgrid=False, automargin=True),
            legend=dict(orientation='h', x=0.5, xanchor='center', y=-0.22,
                        font=dict(size=11), bgcolor='rgba(255,255,255,.9)',
                        bordercolor='#ddd', borderwidth=1))
        fig_g.add_trace(go.Bar(
            name='Programada', y=causas, x=prog_c.values, orientation='h',
            marker=dict(color=C_PROG if grupo=='Interrupciones Programadas' else col_g,
                        opacity=0.75, line=dict(color='white', width=1)),
            text=[str(int(v)) if v>0 else "" for v in prog_c.values],
            textposition='inside', insidetextanchor='middle',
            textfont=dict(color='white', size=10),
            hovertemplate="<b>%{y}</b><br>Programada: %{x}<extra></extra>"))
        fig_g.add_trace(go.Bar(
            name='Forzada', y=causas, x=forz_c.values, orientation='h',
            marker=dict(color=C_FORZ, opacity=0.9, line=dict(color='white', width=1)),
            text=[str(int(v)) if v>0 else "" for v in forz_c.values],
            textposition='inside', insidetextanchor='middle',
            textfont=dict(color='white', size=10),
            hovertemplate="<b>%{y}</b><br>Forzada: %{x}<extra></extra>"))
        for causa, t in zip(causas, tot_c.values):
            if t > 0:
                pct = t / max(len(df10), 1) * 100
                fig_g.add_annotation(
                    x=t, y=causa, text=f"<b>{int(t)}</b> ({pct:.1f}%)",
                    showarrow=False, xanchor='left', yanchor='middle',
                    xshift=7, font=dict(size=10, color='#222'))
        st.plotly_chart(fig_g, use_container_width=True)

    # Evolución mensual
    df_m = df10.copy()
    if 'MES' not in df_m.columns:
        df_m['MES'] = df_m['START'].dt.to_period('M').astype(str)
    agg_mes   = df_m.groupby(['MES','ORIGEN_LABEL']).size().reset_index(name='N')
    meses_ord = sorted(agg_mes['MES'].unique())
    if meses_ord:
        st.markdown("<div class='sec-header'>📅 Evolución Mensual — Cantidad por Origen</div>",
                    unsafe_allow_html=True)
        fig_lin = _fig(
            title=_tit("Interrupciones por Origen — Evolución Mensual"),
            height=420, margin=dict(t=80, b=130, l=60, r=20),
            xaxis=dict(tickmode='array', tickvals=list(range(len(meses_ord))),
                       ticktext=[_mes_fmt(m) for m in meses_ord],
                       tickangle=0, tickfont=dict(size=11), showgrid=False),
            yaxis=dict(title="Eventos", showgrid=True, gridcolor=C_GRID,
                       tickfont=dict(size=11)),
            legend=dict(orientation='h', x=0.5, xanchor='center', y=-0.30,
                        font=dict(size=11), bgcolor='rgba(255,255,255,.9)',
                        bordercolor='#ddd', borderwidth=1))
        for i, org in enumerate(agg_mes['ORIGEN_LABEL'].unique()):
            sub = (agg_mes[agg_mes['ORIGEN_LABEL']==org]
                   .set_index('MES')['N'].reindex(meses_ord, fill_value=0))
            fig_lin.add_trace(go.Scatter(
                x=list(range(len(meses_ord))), y=sub.values,
                mode='lines+markers', name=org,
                line=dict(color=PALETA[i % len(PALETA)], width=2.5),
                marker=dict(size=7), customdata=meses_ord,
                hovertemplate=f"<b>{org}</b><br>%{{customdata}}<br>%{{y}} eventos<extra></extra>"))
        st.plotly_chart(fig_lin, use_container_width=True)

    # Evolución tiempo y frecuencia por calidad
    if df_sir_11 is not None and not df_sir_11.empty:
        meses_sir = sorted(df_sir_10['MES'].unique()) if not df_sir_10.empty else []
        if len(meses_sir) > 1:
            st.markdown("<div class='sec-header'>📅 Evolución Mensual — Tiempo y Frecuencia por Calidad</div>",
                        unsafe_allow_html=True)
            ct1, ct2 = st.columns(2)
            with ct1:
                fig_t = _fig(title=_tit("Tiempo Mensual — Cal. 1 y 2"),
                             height=400, margin=dict(t=80, b=120, l=70, r=20),
                             xaxis=dict(tickmode='array',
                                        tickvals=list(range(len(meses_sir))),
                                        ticktext=[_mes_fmt(m) for m in meses_sir],
                                        tickangle=0, tickfont=dict(size=11), showgrid=False),
                             yaxis=dict(title="h / usuario", showgrid=True,
                                        gridcolor=C_GRID, tickfont=dict(size=11)),
                             legend=dict(orientation='h', x=0.5, xanchor='center',
                                         y=-0.28, font=dict(size=12)))
                for col_t, lim, color, nombre, cs in [
                    ('T_POND_1', LIM_T_MES_C1, C_AZUL,  'Calidad 1', _cs1),
                    ('T_POND_2', LIM_T_MES_C2, C_VERDE, 'Calidad 2', _cs2),
                ]:
                    if col_t not in df_sir_11.columns: continue
                    vals = (df_sir_11.groupby('MES')[col_t].sum() / cs
                            ).reindex(meses_sir, fill_value=0)
                    fig_t.add_trace(go.Scatter(
                        x=list(range(len(meses_sir))), y=vals.values,
                        mode='lines+markers', name=nombre,
                        line=dict(color=color, width=2.5), marker=dict(size=8),
                        customdata=meses_sir,
                        hovertemplate=(f"<b>{nombre}</b><br>%{{customdata}}"
                                       f"<br>%{{y:.4f}} h/usuario<extra></extra>")))
                    fig_t.add_hline(y=lim, line=dict(color=color, width=1.5, dash='dash'),
                                    annotation_text=f"Lím. {nombre}:{lim:.3f}h",
                                    annotation_font=dict(size=10, color=color))
                st.plotly_chart(fig_t, use_container_width=True)
            with ct2:
                fig_f = _fig(title=_tit("Frecuencia Mensual — Cal. 1 y 2"),
                             height=400, margin=dict(t=80, b=120, l=70, r=20),
                             xaxis=dict(tickmode='array',
                                        tickvals=list(range(len(meses_sir))),
                                        ticktext=[_mes_fmt(m) for m in meses_sir],
                                        tickangle=0, tickfont=dict(size=11), showgrid=False),
                             yaxis=dict(title="eventos / usuario", showgrid=True,
                                        gridcolor=C_GRID, tickfont=dict(size=11)),
                             legend=dict(orientation='h', x=0.5, xanchor='center',
                                         y=-0.28, font=dict(size=12)))
                for col_f, lim, color, nombre, cs in [
                    ('CONS_BT_1', LIM_F_MES_C1, C_MORADO, 'Cal. 1', _cs1),
                    ('CONS_BT_2', LIM_F_MES_C2, C_NAR,    'Cal. 2', _cs2),
                ]:
                    if col_f in df_sir_10.columns:
                        vals = (df_sir_10.groupby('MES')[col_f].sum() / cs
                                ).reindex(meses_sir, fill_value=0)
                    else:
                        vals = (df_sir_10.groupby('MES').size() / cs
                                ).reindex(meses_sir, fill_value=0)
                    fig_f.add_trace(go.Scatter(
                        x=list(range(len(meses_sir))), y=vals.values,
                        mode='lines+markers', name=nombre,
                        line=dict(color=color, width=2.5), marker=dict(size=8),
                        customdata=meses_sir,
                        hovertemplate=(f"<b>{nombre}</b><br>%{{customdata}}"
                                       f"<br>%{{y:.4f}} eventos/usuario<extra></extra>")))
                    fig_f.add_hline(y=lim, line=dict(color=color, width=1.5, dash='dash'),
                                    annotation_text=f"Lím. {nombre}:{lim:.3f}",
                                    annotation_font=dict(size=10, color=color))
                st.plotly_chart(fig_f, use_container_width=True)

# ═══════════════════════════════════════════════════════════════════
# CARGA DE ARCHIVOS
# ═══════════════════════════════════════════════════════════════════
st.markdown("<br>", unsafe_allow_html=True)
st.markdown(f"""<div style='background:linear-gradient(90deg,{C_DARK},{C_AZUL});
  color:white;padding:12px 24px;border-radius:12px;font-size:1.0rem;font-weight:700;
  margin-bottom:20px;box-shadow:0 4px 14px rgba(0,40,85,.25)'>
  📂 Carga de Archivos de Datos</div>""", unsafe_allow_html=True)

cu1, cu2, cu3 = st.columns(3)
with cu1:
    st.markdown(
        f"<div style='background:white;border-radius:12px;padding:12px;"
        f"border-left:5px solid {C_AZUL};box-shadow:0 2px 10px rgba(0,0,0,.07);margin-bottom:6px'>"
        f"<b style='color:{C_AZUL}'>📊 ST10 — Interrupciones</b> "
        f"<span style='font-size:.78rem;color:#C62828;font-weight:700'>REQUERIDO</span>"
        f"<br><small style='color:#666'>Tabla maestra de interrupciones</small></div>",
        unsafe_allow_html=True)
    f10_up = st.file_uploader("ST10", type=['mdb'], label_visibility="collapsed")

with cu2:
    st.markdown(
        f"<div style='background:white;border-radius:12px;padding:12px;"
        f"border-left:5px solid {C_VERDE};box-shadow:0 2px 10px rgba(0,0,0,.07);margin-bottom:6px'>"
        f"<b style='color:{C_VERDE}'>📊 ST11 — Repos. BT</b> "
        f"<span style='font-size:.78rem;color:#C62828;font-weight:700'>REQUERIDO</span>"
        f"<br><small style='color:#666'>Reposiciones consumidores BT</small></div>",
        unsafe_allow_html=True)
    f11_up = st.file_uploader("ST11", type=['mdb'], label_visibility="collapsed")

with cu3:
    st.markdown(f"""<div style='background:linear-gradient(135deg,#F3E5F5,#EDE7F6);
      border-radius:12px;padding:12px;border-left:5px solid #7B1FA2;
      box-shadow:0 2px 10px rgba(0,0,0,.07);margin-bottom:6px'>
      <b style='color:#7B1FA2'>📋 Causa Invocada</b>
      <span class='exc-badge'>OPCIONAL</span><br>
      <small style='color:#666'>Excel con columna NUMERO (correlativos a excluir)</small>
      </div>""", unsafe_allow_html=True)
    f_exc_up = st.file_uploader("Excepciones", type=['xlsx','xls'],
                                 label_visibility="collapsed")

# Leer Excel de excepciones
nums_excluir = ()
n_exc_excel  = 0
if f_exc_up is not None:
    try:
        df_exc = pd.read_excel(f_exc_up)
        col_num = next((c for c in df_exc.columns if c.strip().upper() == 'NUMERO'), None)
        if col_num:
            lista = [str(x).strip() for x in df_exc[col_num].dropna()
                     if str(x).strip() not in ('', 'NUMERO')]
            # Normalizar: 12345.0 → '12345'
            lista = [str(int(float(x))) if x.replace('.','',1).isdigit() else x
                     for x in lista]
            nums_excluir = tuple(lista)
            n_exc_excel  = len(nums_excluir)
            st.success(f"✅ Excel cargado: **{n_exc_excel}** correlativos leídos")
        else:
            st.error("❌ El Excel no tiene columna **NUMERO**.")
    except Exception as e:
        st.error(f"Error leyendo Excel: {e}")

# ═══════════════════════════════════════════════════════════════════
# PROCESAMIENTO PRINCIPAL
# ═══════════════════════════════════════════════════════════════════
if f10_up and f11_up:
    b10 = f10_up.read()
    b11 = f11_up.read()

    with st.spinner("⚙️ Procesando datos ST10 + ST11…"):
        resultado = procesar_datos(b10, b11, nums_excluir)

    df10, df_sir_10, df_sir_11, df10_full, df_causa_inv, df_menor3, conteo_st = resultado
    if df10 is None:
        st.stop()

    n10_st, n11_st = conteo_st
    if n10_st != n11_st:
        st.markdown(f"""<div class='warn-box'>
          ⚠️ <b>Verificación ST10 vs ST11:</b>
          ST10 = <b>{n10_st}</b> correlativos únicos |
          ST11 = <b>{n11_st}</b> correlativos únicos.
          Las diferencias se excluyen (inner join).
          </div>""", unsafe_allow_html=True)
    else:
        st.success(f"✅ ST10 y ST11 coinciden: {n10_st} correlativos únicos.")

    # JS — recuerda pestaña activa entre reruns
    components.html("""<script>
    (function(){const K='st_tab_v8';
      function tabs(){return Array.from(document.querySelectorAll('button[data-baseweb="tab"]'));}
      function restore(){const s=sessionStorage.getItem(K);if(!s)return;
        const t=tabs().find(b=>b.textContent.trim().startsWith(s));
        if(t&&t.getAttribute('aria-selected')!=='true')t.click();}
      function save(){const a=document.querySelector('button[data-baseweb="tab"][aria-selected="true"]');
        if(a)sessionStorage.setItem(K,a.textContent.trim().substring(0,4));}
      function init(r){const bs=tabs();
        if(bs.length>0){restore();bs.forEach(b=>b.addEventListener('click',()=>setTimeout(save,100)));}
        else if(r>0)setTimeout(()=>init(r-1),150);}
      window.addEventListener('load',()=>init(20));
      new MutationObserver(()=>init(10)).observe(document.body,{childList:true,subtree:true});
    })();</script>""", height=0, scrolling=False)

    # ── PESTAÑAS ──────────────────────────────────────────────────
    (tab_res, tab_graf, tab_pct,
     tab_ver, tab_ext, tab_inv,
     tab_umb, tab_bib, tab_adm) = st.tabs([
        "📊 Indicadores Globales",
        "📈 Gráficas y Métricas",
        "🥧 Por Origen y Causa",
        "📋 Historial",
        "🚫 Origen Externo",
        "📌 Causa Invocada",
        "⏲️ Umbral",
        "📚 Biblioteca",
        "👤 Admin",
    ])

    # ══════════════════════════════════════════════════════════════
    # TAB 1 — CALIDADES DE SERVICIO
    # ══════════════════════════════════════════════════════════════
    with tab_res:
        mostrar_kpis(df10, df_causa_inv, df_menor3)
        st.markdown("<br>", unsafe_allow_html=True)
        res_c1 = armar_tabla_sir(df_sir_10, df_sir_11, cs_1, 1)
        res_c2 = armar_tabla_sir(df_sir_10, df_sir_11, cs_2, 2)

        st.markdown("<div class='sec-header'>📊 Informe de Indicadores de Continuidad SIR</div>",
                    unsafe_allow_html=True)

        if nums_excluir and df_causa_inv is not None and not df_causa_inv.empty:
            n_real = len(df_causa_inv)
            st.markdown(f"""<div class='exc-box'>
              <b style='color:#7B1FA2'>ℹ️ Causa Invocada aplicada:</b>
              Se leyeron <b>{n_exc_excel}</b> correlativos del Excel.
              De ellos, <b>{n_real}</b> coincidieron con ST10 y fueron excluidos del SIR.
              Ver detalle en <b>📌 Causa Invocada</b>.
              </div>""", unsafe_allow_html=True)
        elif nums_excluir:
            st.info(f"Se cargaron {n_exc_excel} correlativos del Excel pero ninguno coincidió con ST10.")

        _, dl2, dl3 = st.columns([3, 1, 1])
        with dl2:
            st.download_button("📄 Reporte HTML", data=generar_html(res_c1, res_c2),
                               file_name="Reporte_Indicadores.html", mime="text/html",
                               use_container_width=True)
        with dl3:
            st.download_button("📥 Excel",
                data=to_excel_multi({
                    'Calidad1': res_c1.reset_index().rename(columns={'index':'ORIGEN'}),
                    'Calidad2': res_c2.reset_index().rename(columns={'index':'ORIGEN'}),
                }),
                file_name="SIR_Global.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)

        def _estilo(df):
            return (df.style
                    .format("{:.4f}")
                    .set_properties(**{'text-align': 'center'})
                    .highlight_max(axis=0, color='#FFCDD2', subset=df.columns)
                    .apply(lambda x: ['background-color:#E8F5E9;font-weight:700'
                                      if x.name == 'TOTAL DIST.' else '' for _ in x], axis=1)
                    .set_table_styles([{'selector':'th',
                        'props':[('background-color', C_DARK),
                                 ('color', 'white'), ('font-weight', '700')]}]))

        st.markdown(
            f"<div style='background:linear-gradient(90deg,{C_AZUL}18,{C_AZUL}06);"
            f"border-left:5px solid {C_AZUL};border-radius:10px;"
            f"padding:11px 20px;margin:18px 0 10px'>"
            f"<span style='font-size:.95rem;font-weight:800;color:{C_DARK}'>🔹 CALIDAD 1</span>"
            f"&nbsp;<span style='font-size:.80rem;color:#666'>"
            f"— {cs_1:,.3f} consumidores · Lím. T: {LIM_T_SEM_C1}h · Lím. F: {LIM_F_SEM_C1}</span>"
            f"</div>", unsafe_allow_html=True)
        st.dataframe(_estilo(res_c1), use_container_width=True)

        st.markdown(
            f"<div style='background:linear-gradient(90deg,{C_VERDE}18,{C_VERDE}06);"
            f"border-left:5px solid {C_VERDE};border-radius:10px;"
            f"padding:11px 20px;margin:22px 0 10px'>"
            f"<span style='font-size:.95rem;font-weight:800;color:{C_DARK}'>🔹 CALIDAD 2</span>"
            f"&nbsp;<span style='font-size:.80rem;color:#666'>"
            f"— {cs_2:,.3f} consumidores · Lím. T: {LIM_T_SEM_C2}h · Lím. F: {LIM_F_SEM_C2}</span>"
            f"</div>", unsafe_allow_html=True)
        st.dataframe(_estilo(res_c2), use_container_width=True)

        if df_menor3 is not None and not df_menor3.empty:
            st.markdown("<br>", unsafe_allow_html=True)
            with st.expander(f"ℹ️ Interrupciones < 3 minutos — {len(df_menor3)} registros (excluidas del SIR)"):
                cols_m = [c for c in ['NUMERO','START','END','DURACION_H',
                                       'ORIGEN_LABEL','CAUSA_LABEL','TIPO_LABEL']
                          if c in df_menor3.columns]
                st.dataframe(df_menor3[cols_m].style.format({'DURACION_H':'{:.4f}'}),
                             use_container_width=True, height=300)
                st.download_button("📥 Exportar < 3 min",
                    data=to_excel(df_menor3[cols_m]), file_name="Menores3min.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ══════════════════════════════════════════════════════════════
    # TAB 2 — GRÁFICAS Y MÉTRICAS
    # ══════════════════════════════════════════════════════════════
    with tab_graf:
        st.markdown("<div class='sec-header'>🏗️ Análisis de Impacto por Origen y Tipo</div>",
                    unsafe_allow_html=True)
        graficar_impacto_origen(df_sir_10, df_sir_11)
        st.markdown("<hr style='border:none;border-top:2px solid #CDD5E0;margin:30px 0'>",
                    unsafe_allow_html=True)
        st.markdown("<div class='sec-header'>📅 Incidencia Mensual vs Límites Normativos</div>",
                    unsafe_allow_html=True)
        graficar_incidencia_mensual(df_sir_10, df_sir_11, cs_1, cs_2)

    # ══════════════════════════════════════════════════════════════
    # TAB 3 — POR ORIGEN Y CAUSA
    # ══════════════════════════════════════════════════════════════
    with tab_pct:
        graficar_origen_causa(df10, df_sir_10, df_sir_11, cs_1, cs_2)

    # ══════════════════════════════════════════════════════════════
    # TAB 4 — HISTORIAL
    # ══════════════════════════════════════════════════════════════
    with tab_ver:
        st.markdown("<div class='sec-header'>📋 Historial Total de Interrupciones</div>",
                    unsafe_allow_html=True)
        total_ev = len(df10)
        prog_ev  = int((df10['TIPO_LABEL'] == 'PROGRAMADA').sum())
        forz_ev  = total_ev - prog_ev
        m1, m2, m3, m4 = st.columns(4)
        m1.metric("Total",             f"{total_ev:,}")
        m2.metric("Programadas",       f"{prog_ev:,}")
        m3.metric("Forzadas",          f"{forz_ev:,}")
        m4.metric("Duración promedio", f"{df10['DURACION_H'].mean():.2f} h")
        st.markdown("<br>", unsafe_allow_html=True)
        cols_h = [c for c in ['NUMERO','START','END','DURACION_H',
                               'ORIGEN_LABEL','CAUSA_LABEL','GRUPO_CAUSA','TIPO_LABEL']
                  if c in df10.columns]
        st.dataframe(df10[cols_h].sort_values('NUMERO')
                     .style.format({'DURACION_H':'{:.3f}'}),
                     use_container_width=True, height=500)
        st.download_button("📥 Exportar Historial",
            data=to_excel(df10[cols_h].sort_values('NUMERO')),
            file_name="Historial.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ══════════════════════════════════════════════════════════════
    # TAB 5 — ORIGEN EXTERNO
    # ══════════════════════════════════════════════════════════════
    with tab_ext:
        st.markdown("<div class='sec-header'>🚫 Interrupciones de Origen Externo (Cód. 10, 11, 12)</div>",
                    unsafe_allow_html=True)
        cols_e = [c for c in ['NUMERO','START','END','DURACION_H',
                               'COD_ORIGEN','CAUSA_LABEL','TIPO_LABEL']
                  if c in df10_full.columns]
        df_ext = df10_full[df10_full['ORIGEN_LABEL'] == 'EXTERNO'][cols_e].copy()
        c1, c2, c3 = st.columns(3)
        c1.metric("Eventos externos",  f"{len(df_ext):,}")
        c2.metric("Duración total",
                  f"{df_ext['DURACION_H'].sum():.2f} h" if not df_ext.empty else "0 h")
        c3.metric("Duración promedio",
                  f"{df_ext['DURACION_H'].mean():.2f} h" if not df_ext.empty else "0 h")
        st.markdown("<br>", unsafe_allow_html=True)
        if df_ext.empty:
            st.success("No se registraron interrupciones de origen externo.")
        else:
            st.dataframe(df_ext.style.format({'DURACION_H':'{:.3f}'}),
                         use_container_width=True, height=450)
            st.download_button("📥 Exportar", data=to_excel(df_ext),
                file_name="OrigenExterno.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ══════════════════════════════════════════════════════════════
    # TAB 6 — CAUSA INVOCADA
    # ══════════════════════════════════════════════════════════════
    with tab_inv:
        st.markdown("<div class='sec-header'>📌 Causa Invocada — Interrupciones Excluidas del SIR</div>",
                    unsafe_allow_html=True)
        if not nums_excluir:
            st.markdown(f"""<div style='text-align:center;padding:36px;
              background:linear-gradient(135deg,#F3E5F5,#EDE7F6);
              border:2px dashed #CE93D8;border-radius:14px'>
              <div style='font-size:2.5rem'>📋</div>
              <h3 style='color:#7B1FA2'>No se cargó archivo de excepciones</h3>
              <p style='color:#666'>Cargue un Excel con columna <b>NUMERO</b>.</p>
              </div>""", unsafe_allow_html=True)
        elif df_causa_inv is None or df_causa_inv.empty:
            st.warning(f"Se cargaron {n_exc_excel} correlativos pero ninguno coincidió con ST10.")
        else:
            n_real = len(df_causa_inv)
            c1, c2, c3, c4 = st.columns(4)
            c1.metric("Excluidos del SIR",       f"{n_real:,}")
            c2.metric("Leídos del Excel",          f"{n_exc_excel:,}")
            c3.metric("Duración total excluida",   f"{df_causa_inv['DURACION_H'].sum():.2f} h")
            c4.metric("% del total",               f"{n_real/max(len(df10_full),1)*100:.1f}%")
            st.markdown(f"""<div class='exc-box' style='margin-top:16px'>
              <b style='color:#7B1FA2'>ℹ️ Causa Invocada:</b>
              De los <b>{n_exc_excel}</b> correlativos del Excel,
              <b>{n_real}</b> coincidieron con ST10 y fueron excluidos del SIR.
              </div>""", unsafe_allow_html=True)
            cols_i = [c for c in ['NUMERO','START','END','DURACION_H',
                                   'ORIGEN_LABEL','CAUSA_LABEL','TIPO_LABEL']
                      if c in df_causa_inv.columns]
            st.dataframe(df_causa_inv[cols_i].sort_values('NUMERO')
                         .style.format({'DURACION_H':'{:.3f}'}),
                         use_container_width=True, height=450)
            cg1, cg2 = st.columns(2)
            with cg1:
                agg_iv = df_causa_inv.groupby('ORIGEN_LABEL').size().reset_index(name='N')
                fig_iv = _fig(title=_tit("Causa Invocada — por Origen"), height=320,
                              margin=dict(t=65, b=60, l=20, r=20), showlegend=True,
                              legend=dict(orientation='h', x=0.5, xanchor='center',
                                          y=-0.20, font=dict(size=11)))
                fig_iv.add_trace(go.Pie(
                    labels=agg_iv['ORIGEN_LABEL'], values=agg_iv['N'], hole=0.42,
                    marker=dict(colors=PALETA, line=dict(color='white', width=2)),
                    textinfo='label+percent', textfont=dict(size=11),
                    hovertemplate="<b>%{label}</b><br>%{value}<br>%{percent}<extra></extra>"))
                st.plotly_chart(fig_iv, use_container_width=True)
            with cg2:
                agg_iv2 = df_causa_inv.groupby('TIPO_LABEL').size().reset_index(name='N')
                ct2c = [C_PROG if t=='PROGRAMADA' else C_FORZ for t in agg_iv2['TIPO_LABEL']]
                fig_iv2 = _fig(title=_tit("Causa Invocada — Prog. vs Forz."), height=320,
                               margin=dict(t=65, b=60, l=20, r=20), showlegend=True,
                               legend=dict(orientation='h', x=0.5, xanchor='center',
                                           y=-0.20, font=dict(size=11)))
                fig_iv2.add_trace(go.Pie(
                    labels=agg_iv2['TIPO_LABEL'], values=agg_iv2['N'], hole=0.42,
                    marker=dict(colors=ct2c, line=dict(color='white', width=2)),
                    textinfo='label+percent', textfont=dict(size=12),
                    hovertemplate="<b>%{label}</b><br>%{value}<br>%{percent}<extra></extra>"))
                st.plotly_chart(fig_iv2, use_container_width=True)
            st.download_button("📥 Exportar Causa Invocada",
                data=to_excel(df_causa_inv[cols_i].sort_values('NUMERO')),
                file_name="CausaInvocada.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # ══════════════════════════════════════════════════════════════
    # TAB 7 — ANÁLISIS POR UMBRAL
    # ══════════════════════════════════════════════════════════════
    with tab_umb:
        st.markdown("<div class='sec-header'>⏲️ Análisis por Umbral de Duración</div>",
                    unsafe_allow_html=True)
        st.markdown("<div class='norm-box'>Filtre interrupciones por duración y tipo. "
                    "Puede incluir o excluir Origen Externo y Causa Invocada.</div>",
                    unsafe_allow_html=True)
        fl1, fl2, fl3, fl4 = st.columns([1, 1, 1, 1])
        with fl1:
            st.number_input("Duración mayor a (horas):", min_value=0, step=1, key='h_lim')
        with fl2:
            inc_ext = st.selectbox("Origen Externo", ["Excluir","Incluir"], index=0)
        with fl3:
            inc_inv = st.selectbox("Causa Invocada", ["Excluir","Incluir"], index=0)
        with fl4:
            tipo_flt = st.selectbox("Tipo", ["Todos","FORZADA","PROGRAMADA"], index=0)

        h_usar   = st.session_state['h_lim']
        df_base  = df10_full.copy()
        if inc_ext == "Excluir":
            df_base = df_base[df_base['ORIGEN_LABEL'] != 'EXTERNO']
        if inc_inv == "Excluir" and df_causa_inv is not None and not df_causa_inv.empty:
            exc_nums = set(df_causa_inv['NUMERO'].astype(str))
            df_base  = df_base[~df_base['NUMERO'].astype(str).isin(exc_nums)]
        if tipo_flt != "Todos":
            df_base = df_base[df_base['TIPO_LABEL'] == tipo_flt]
        df_filtrado = df_base[df_base['DURACION_H'] > h_usar].copy()

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Eventos mostrados", f"{len(df_filtrado):,}")
        c2.metric("Umbral",            f"> {h_usar} h")
        c3.metric("Duración total",
                  f"{df_filtrado['DURACION_H'].sum():.2f} h" if not df_filtrado.empty else "0 h")
        c4.metric("% del total ST10",
                  f"{len(df_filtrado)/max(len(df10_full),1)*100:.1f}%")

        st.markdown("<br>", unsafe_allow_html=True)
        if df_filtrado.empty:
            st.success(f"✅ No hay interrupciones mayores a {h_usar} h con los filtros aplicados.")
        else:
            cols_u = [c for c in ['NUMERO','DURACION_H','ORIGEN_LABEL',
                                   'CAUSA_LABEL','GRUPO_CAUSA','TIPO_LABEL','START']
                      if c in df_filtrado.columns]
            st.dataframe(df_filtrado[cols_u].sort_values('DURACION_H', ascending=False)
                         .style.format({'DURACION_H':'{:.3f}'}),
                         use_container_width=True, height=520)
            st.download_button("📥 Exportar Filtrado",
                data=to_excel(df_filtrado[cols_u].sort_values('DURACION_H', ascending=False)),
                file_name=f"Umbral_{h_usar}h.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True)

    # ══════════════════════════════════════════════════════════════
    # TAB 8 — BIBLIOTECA (guarda resultados + tablas crudas)
    # ══════════════════════════════════════════════════════════════
    with tab_bib:
        st.markdown("<div class='sec-header'>📚 Biblioteca de Reportes Guardados</div>",
                    unsafe_allow_html=True)
        usuario_act = st.session_state['usuario_actual']

        # ── Guardar reporte actual ───────────────────────────────
        st.markdown("##### 💾 Guardar cálculo actual en la biblioteca")
        col_n, col_btn = st.columns([3, 1])
        with col_n:
            nombre_rep = st.text_input("Nombre del reporte:",
                                        placeholder="Ej: Semestre 1 — Villazón 2026",
                                        label_visibility="visible")
        with col_btn:
            st.markdown("<div style='height:28px'></div>", unsafe_allow_html=True)
            if st.button("💾 Guardar", use_container_width=True):
                if nombre_rep.strip():
                    # Incluir tablas crudas (bytes) para poder recalcular
                    datos_rep = {
                        'nombre':    nombre_rep,
                        'usuario':   usuario_act,
                        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        'cs_1':      cs_1,
                        'cs_2':      cs_2,
                        # Resultados calculados
                        'df10':          df10,
                        'df_sir_10':     df_sir_10,
                        'df_sir_11':     df_sir_11,
                        'df10_full':     df10_full,
                        'df_causa_inv':  df_causa_inv,
                        'df_menor3':     df_menor3,
                        # Tablas crudas (bytes originales de los .mdb)
                        'raw_st10':      b10,
                        'raw_st11':      b11,
                        'nums_excluir':  nums_excluir,
                        '_tablas_guardadas': ['ST10','ST11'] +
                            (['Excel Causa Invocada'] if nums_excluir else []),
                    }
                    if guardar_reporte(nombre_rep, usuario_act, datos_rep):
                        st.success(f"✅ Reporte **{nombre_rep}** guardado con tablas ST10 y ST11.")
                else:
                    st.warning("Ingrese un nombre para el reporte.")

        st.markdown("<hr style='border:none;border-top:1px solid #ddd;margin:20px 0'>",
                    unsafe_allow_html=True)
        st.markdown("##### 📂 Mis reportes guardados")

        idx       = leer_indice()
        mis_rep   = [r for r in idx if r['usuario'] == usuario_act]
        if not mis_rep:
            st.info("No tiene reportes guardados aún.")
        else:
            for rep in reversed(mis_rep):
                tablas_info = ', '.join(rep.get('tablas', []))
                col_r1, col_r2, col_r3 = st.columns([4, 1, 1])
                with col_r1:
                    st.markdown(
                        f"<div class='bib-card'>"
                        f"<span style='font-size:1.3rem'>📄</span>"
                        f"<div><b>{rep['nombre']}</b>"
                        f"<br><span style='color:#888;font-size:.80rem'>"
                        f"{rep['timestamp']}"
                        f"{' · ' + tablas_info if tablas_info else ''}"
                        f"</span></div></div>",
                        unsafe_allow_html=True)
                with col_r2:
                    if st.button("📂 Cargar", key=f"load_{rep['archivo']}",
                                 use_container_width=True):
                        datos = cargar_reporte(rep['archivo'])
                        if datos:
                            st.session_state['reporte_activo'] = datos
                            registrar_log(usuario_act, 'CARGAR_REPORTE', rep['nombre'])
                            st.success(f"✅ **{rep['nombre']}** cargado.")
                with col_r3:
                    if st.button("🗑️ Eliminar", key=f"del_{rep['archivo']}",
                                 use_container_width=True):
                        if eliminar_reporte(rep['archivo']):
                            st.success("Eliminado.")
                            st.rerun()

        # ── Mostrar reporte cargado ──────────────────────────────
        if st.session_state.get('reporte_activo'):
            rep_dat = st.session_state['reporte_activo']
            st.markdown("<hr style='border:none;border-top:1px solid #ddd;margin:20px 0'>",
                        unsafe_allow_html=True)
            st.markdown(
                f"##### 📋 Reporte cargado: **{rep_dat.get('nombre','')}**"
                f" — {rep_dat.get('timestamp','')}")

            if rep_dat.get('df10') is not None:
                cm1, cm2, cm3 = st.columns(3)
                cm1.metric("Total interrupciones", f"{len(rep_dat['df10']):,}")
                cm2.metric("Consumidores Cal.1",   f"{rep_dat.get('cs_1',0):,.3f}")
                cm3.metric("Consumidores Cal.2",   f"{rep_dat.get('cs_2',0):,.3f}")

                # Indicar qué tablas están guardadas
                tablas = rep_dat.get('_tablas_guardadas', [])
                if tablas:
                    st.info(f"📦 Tablas almacenadas: **{', '.join(tablas)}**")

                if rep_dat.get('df_sir_10') is not None and rep_dat.get('df_sir_11') is not None:
                    rr1 = armar_tabla_sir(rep_dat['df_sir_10'], rep_dat['df_sir_11'],
                                          rep_dat['cs_1'], 1)
                    rr2 = armar_tabla_sir(rep_dat['df_sir_10'], rep_dat['df_sir_11'],
                                          rep_dat['cs_2'], 2)
                    st.markdown("**Calidad 1:**")
                    st.dataframe(rr1.style.format("{:.4f}"), use_container_width=True)
                    st.markdown("**Calidad 2:**")
                    st.dataframe(rr2.style.format("{:.4f}"), use_container_width=True)
                    st.download_button(
                        "📥 Exportar reporte guardado",
                        data=to_excel_multi({'Cal1': rr1.reset_index(), 'Cal2': rr2.reset_index()}),
                        file_name=f"{rep_dat.get('nombre','reporte').replace(' ','_')}.xlsx",
                        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

            # Si tiene tablas crudas, permitir re-procesar
            if rep_dat.get('raw_st10') and rep_dat.get('raw_st11'):
                st.markdown("---")
                st.markdown("**🔄 Re-procesar tablas guardadas:**")
                if st.button("⚙️ Recalcular desde tablas guardadas", use_container_width=False):
                    with st.spinner("Recalculando…"):
                        res_new = procesar_datos(
                            rep_dat['raw_st10'], rep_dat['raw_st11'],
                            rep_dat.get('nums_excluir', ()))
                    if res_new[0] is not None:
                        st.success("✅ Recálculo completado con los datos originales guardados.")

    # ══════════════════════════════════════════════════════════════
    # TAB 9 — PANEL ADMINISTRADOR (SOLO crollano)
    # ══════════════════════════════════════════════════════════════
    with tab_adm:
        if st.session_state['usuario_actual'] != ADMIN_USER:
            # Mensaje genérico — no revela que hay un panel oculto
            st.error("🔒 Esta sección no está disponible para su usuario.")
        else:
            st.markdown(f"<div class='sec-header'>👤 Panel de Administrador — {ADMIN_USER}</div>",
                        unsafe_allow_html=True)
            st.markdown(
                f"<div style='background:#FFF8E1;border:1px solid #FFB300;"
                f"border-radius:10px;padding:10px 18px;margin-bottom:16px;font-size:.86rem'>"
                f"🔐 Esta sección es <b>privada</b>. Solo usted puede verla.</div>",
                unsafe_allow_html=True)

            # ── Registro de accesos ──────────────────────────────
            st.markdown("##### 📋 Registro Completo de Accesos al Sistema")
            log = leer_log()
            if log:
                df_log = pd.DataFrame(log).sort_values('timestamp', ascending=False)
                # Métricas rápidas
                logins_tot = len(df_log[df_log['accion']=='LOGIN'])
                usuarios_unicos = df_log[df_log['accion']=='LOGIN']['usuario'].nunique()
                ultimo = df_log.iloc[0]['timestamp'] if not df_log.empty else '—'
                lm1, lm2, lm3 = st.columns(3)
                lm1.metric("Total ingresos",    f"{logins_tot:,}")
                lm2.metric("Usuarios distintos", f"{usuarios_unicos}")
                lm3.metric("Último acceso",      ultimo)
                st.markdown("<br>", unsafe_allow_html=True)
                st.dataframe(df_log, use_container_width=True, height=380)
                st.download_button(
                    "📥 Exportar Log de Accesos",
                    data=to_excel(df_log),
                    file_name=f"Log_Accesos_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("Sin registros de acceso aún.")

            # ── Gráfica ingresos por usuario ────────────────────
            if log:
                st.markdown("<br>", unsafe_allow_html=True)
                df_log2  = pd.DataFrame(log)
                logins   = df_log2[df_log2['accion'] == 'LOGIN']
                agg_u    = logins.groupby('usuario').size().reset_index(name='Ingresos')
                fig_u    = _fig(title=_tit("Ingresos al sistema por usuario"),
                                height=300, margin=dict(t=70, b=40, l=20, r=20))
                fig_u.add_trace(go.Bar(
                    x=agg_u['usuario'], y=agg_u['Ingresos'],
                    marker=dict(color=PALETA[:len(agg_u)], line=dict(color='white', width=1)),
                    text=agg_u['Ingresos'], textposition='outside',
                    hovertemplate="<b>%{x}</b><br>Ingresos: %{y}<extra></extra>"))
                st.plotly_chart(fig_u, use_container_width=True)

            # ── Todos los reportes de todos los usuarios ────────
            st.markdown("<hr style='border:none;border-top:1px solid #ddd;margin:20px 0'>",
                        unsafe_allow_html=True)
            st.markdown("##### 📚 Todos los Reportes — Todos los Usuarios")
            idx_all = leer_indice()
            if not idx_all:
                st.info("No hay reportes guardados en el sistema.")
            else:
                df_idx = pd.DataFrame(idx_all)
                st.dataframe(df_idx[['nombre','usuario','timestamp']],
                             use_container_width=True)
                for rep in reversed(idx_all):
                    ca, cb, cc = st.columns([4, 1, 1])
                    with ca:
                        st.markdown(
                            f"📄 **{rep['nombre']}** | 👤 {rep['usuario']} | {rep['timestamp']}",
                            unsafe_allow_html=True)
                    with cb:
                        if st.button("📂 Ver", key=f"adm_load_{rep['archivo']}",
                                     use_container_width=True):
                            datos = cargar_reporte(rep['archivo'])
                            if datos:
                                st.session_state['reporte_activo'] = datos
                                st.success("Cargado.")
                    with cc:
                        if st.button("🗑️ Eliminar", key=f"adm_del_{rep['archivo']}",
                                     use_container_width=True):
                            if eliminar_reporte(rep['archivo']):
                                st.success("Eliminado.")
                                st.rerun()

# ═══════════════════════════════════════════════════════════════════
# PANTALLA INICIAL (sin archivos cargados)
# ═══════════════════════════════════════════════════════════════════
else:
    st.markdown(f"""<div style='text-align:center;padding:70px 20px 50px;background:white;
      border-radius:20px;margin-top:20px;box-shadow:0 4px 24px rgba(0,40,85,.1)'>
      <div style='font-size:4.5rem;margin-bottom:16px'>📂</div>
      <h3 style='color:{C_DARK};margin-bottom:12px'>Cargue ST10 y ST11 para comenzar</h3>
      <p style='color:#777;font-size:.93rem;max-width:560px;margin:0 auto;line-height:1.8'>
      Los archivos <b>ST10</b> y <b>ST11</b> son obligatorios para calcular los indicadores.<br>
      El Excel de <b>Causa Invocada</b> (columna NUMERO) es opcional.<br><br>
      <span style='font-size:.82rem;color:#aaa'>Desarrollado por
      <b style='color:{C_AZUL}'>Ing. Cristian Braulio Rollano M.</b></span>
      </p></div>""", unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)
    tb_sin, tb_sin2 = st.tabs(["📚 Biblioteca", "👤 Admin"])

    with tb_sin:
        st.markdown("<div class='sec-header'>📚 Biblioteca de Reportes</div>",
                    unsafe_allow_html=True)
        usuario_act = st.session_state['usuario_actual']
        idx = leer_indice()
        mis_rep = [r for r in idx if r['usuario'] == usuario_act]
        if not mis_rep:
            st.info("No tiene reportes guardados. Cargue ST10 y ST11 para procesar y guardar.")
        else:
            for rep in reversed(mis_rep):
                tablas_info = ', '.join(rep.get('tablas', []))
                cr1, cr2, cr3 = st.columns([4, 1, 1])
                with cr1:
                    st.markdown(
                        f"<div class='bib-card'><span style='font-size:1.3rem'>📄</span>"
                        f"<div><b>{rep['nombre']}</b><br>"
                        f"<span style='color:#888;font-size:.80rem'>{rep['timestamp']}"
                        f"{' · ' + tablas_info if tablas_info else ''}</span></div></div>",
                        unsafe_allow_html=True)
                with cr2:
                    if st.button("📂 Cargar", key=f"load2_{rep['archivo']}",
                                 use_container_width=True):
                        datos = cargar_reporte(rep['archivo'])
                        if datos:
                            st.session_state['reporte_activo'] = datos
                            st.success("Cargado.")
                with cr3:
                    if st.button("🗑️ Eliminar", key=f"del2_{rep['archivo']}",
                                 use_container_width=True):
                        if eliminar_reporte(rep['archivo']): st.rerun()

        if st.session_state.get('reporte_activo'):
            rep_dat = st.session_state['reporte_activo']
            st.markdown(f"##### 📋 Reporte: **{rep_dat.get('nombre','')}** "
                        f"— {rep_dat.get('timestamp','')}")
            if rep_dat.get('df_sir_10') is not None and rep_dat.get('df_sir_11') is not None:
                rr1 = armar_tabla_sir(rep_dat['df_sir_10'], rep_dat['df_sir_11'],
                                      rep_dat['cs_1'], 1)
                rr2 = armar_tabla_sir(rep_dat['df_sir_10'], rep_dat['df_sir_11'],
                                      rep_dat['cs_2'], 2)
                st.markdown("**Calidad 1:**")
                st.dataframe(rr1.style.format("{:.4f}"), use_container_width=True)
                st.markdown("**Calidad 2:**")
                st.dataframe(rr2.style.format("{:.4f}"), use_container_width=True)

    with tb_sin2:
        if st.session_state['usuario_actual'] != ADMIN_USER:
            st.error("🔒 Esta sección no está disponible para su usuario.")
        else:
            st.markdown("<div class='sec-header'>👤 Panel de Administrador</div>",
                        unsafe_allow_html=True)
            log = leer_log()
            if log:
                df_log = pd.DataFrame(log).sort_values('timestamp', ascending=False)
                st.dataframe(df_log, use_container_width=True, height=400)
                st.download_button(
                    "📥 Exportar Log",
                    data=to_excel(df_log),
                    file_name=f"Log_{datetime.now().strftime('%Y%m%d')}.xlsx",
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
            else:
                st.info("Sin registros de acceso.")
