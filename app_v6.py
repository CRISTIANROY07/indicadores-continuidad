import streamlit as st
import pandas as pd
import numpy as np
import pyodbc
import tempfile
import os
import io
import plotly.graph_objects as go

# ════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ════════════════════════════════════════════════════════════
st.set_page_config(page_title="Indicadores de Continuidad", layout="wide", page_icon="⚡")

USUARIOS     = {"crollano": "admin123", "ingeniero1": "clave1", "operaciones": "operaciones2026"}
CAUSAS_PROG  = {'20', '21', '22', '23', '24'}
MAPA_ORIGEN  = {
    '01': 'SUBTRANSMISIÓN', '02': 'DIST. PRIMARIA', '03': 'DIST. SECUNDARIA',
    '10': 'EXTERNO',        '11': 'EXTERNO',         '12': 'EXTERNO'
}

LIM_T_SEM_C1 = 6.0;   LIM_T_MES_C1 = LIM_T_SEM_C1 / 6
LIM_F_SEM_C1 = 7.0;   LIM_F_MES_C1 = LIM_F_SEM_C1 / 6
LIM_T_SEM_C2 = 12.0;  LIM_T_MES_C2 = LIM_T_SEM_C2 / 6
LIM_F_SEM_C2 = 14.0;  LIM_F_MES_C2 = LIM_F_SEM_C2 / 6

C_AZUL    = "#1565C0"
C_VERDE   = "#00695C"
C_MORADO  = "#4527A0"
C_NARANJA = "#E65100"
C_ROJO    = "#B71C1C"
C_AMBER   = "#E65100"
C_PROG    = "#1976D2"
C_FORZ    = "#C62828"
C_BG      = "#F2F5FA"
C_GRID    = "rgba(150,150,150,0.2)"
C_WHITE   = "#FFFFFF"
C_DARK    = "#002855"

# ════════════════════════════════════════════════════════════
# SESSION STATE INICIAL
# ════════════════════════════════════════════════════════════
_defaults = {
    'logueado':       False,
    'usuario_actual': '',
    'h_lim':          2,        # valor del filtro de umbral — persiste entre reruns
    'tab_activa':     0,
}
for k, v in _defaults.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ════════════════════════════════════════════════════════════
# LOGIN
# ════════════════════════════════════════════════════════════
if not st.session_state['logueado']:
    st.markdown("""<style>
    .stApp{background:linear-gradient(135deg,#001533 0%,#003080 60%,#0057B7 100%)!important}
    /* Etiquetas de inputs en el login bien visibles sobre fondo oscuro */
    label[data-testid="stWidgetLabel"] p{color:#002855 !important;font-weight:600 !important}
    </style>""", unsafe_allow_html=True)

    _, col_mid, _ = st.columns([1, 1.1, 1])
    with col_mid:
        st.markdown("<br><br>", unsafe_allow_html=True)

        # Tarjeta de login en HTML (solo decorativa, sin inputs)
        st.markdown("""
        <div style='background:white;border-radius:18px;padding:32px 40px 20px;
                    box-shadow:0 24px 80px rgba(0,0,0,0.45);'>
          <div style='text-align:center;margin-bottom:22px'>
            <div style='font-size:3rem'>⚡</div>
            <h2 style='color:#002855;margin:6px 0 4px;font-family:Segoe UI;font-size:1.45rem;font-weight:700'>
              Sistema de Indicadores de Continuidad</h2>
            <p style='color:#888;font-size:.85rem;margin:0'>
              Acceso restringido — Ingrese sus credenciales corporativas</p>
          </div>
        </div>""", unsafe_allow_html=True)

        # Form con labels explícitos y placeholder para mayor claridad
        with st.form("login_form", clear_on_submit=False):
            st.markdown(
                "<p style='color:#002855;font-weight:700;font-size:.95rem;"
                "margin:6px 0 2px'>👤 Nombre de usuario</p>",
                unsafe_allow_html=True,
            )
            user = st.text_input(
                label="usuario_input",
                placeholder="Escriba su usuario corporativo…",
                label_visibility="collapsed",
            )

            st.markdown(
                "<p style='color:#002855;font-weight:700;font-size:.95rem;"
                "margin:14px 0 2px'>🔑 Contraseña</p>",
                unsafe_allow_html=True,
            )
            pwd = st.text_input(
                label="pwd_input",
                placeholder="Escriba su contraseña…",
                type="password",
                label_visibility="collapsed",
            )

            st.markdown("<br>", unsafe_allow_html=True)
            if st.form_submit_button("▶  Ingresar al Sistema", use_container_width=True):
                if USUARIOS.get(user) == pwd:
                    st.session_state['logueado']       = True
                    st.session_state['usuario_actual'] = user
                    st.rerun()
                else:
                    st.error("⚠️ Usuario o contraseña incorrectos. Vuelva a intentarlo.")
    st.stop()

# ════════════════════════════════════════════════════════════
# ESTILOS GLOBALES (post-login)
# ════════════════════════════════════════════════════════════
st.markdown(f"""<style>
.stApp{{background:linear-gradient(160deg,#ECF0F8 0%,#E2E8F2 100%)}}
h1,h2,h3,h4{{color:{C_DARK};font-family:'Segoe UI',sans-serif}}

.creator-badge{{
    background:linear-gradient(90deg,{C_DARK} 0%,{C_AZUL} 100%);
    color:white;padding:5px 18px;border-radius:30px;font-size:.88rem;
    font-weight:600;display:inline-block;
    box-shadow:0 4px 14px rgba(0,40,85,.28);margin-bottom:12px
}}

.sec-header{{
    background:linear-gradient(90deg,{C_DARK} 0%,#0044AA 100%);
    color:white;padding:11px 22px;border-radius:10px;font-size:1.0rem;
    font-weight:700;margin:26px 0 16px;
    box-shadow:0 4px 12px rgba(0,40,85,.22);letter-spacing:.3px
}}

.norm-box{{
    background:white;border-left:5px solid {C_AZUL};border-radius:10px;
    padding:14px 22px;margin-bottom:18px;
    box-shadow:0 2px 12px rgba(0,0,0,.07);font-size:.91rem;color:#333
}}

.norm-table{{width:100%;border-collapse:collapse;margin-top:10px;font-size:.91rem}}
.norm-table th{{
    background:{C_DARK};color:white;padding:8px 14px;
    text-align:center;font-weight:600;letter-spacing:.3px
}}
.norm-table td{{padding:8px 14px;text-align:center;border-bottom:1px solid #eee}}
.norm-table tr:nth-child(even){{background:#F6F9FF}}
.norm-table td:first-child{{text-align:left;font-weight:600;color:{C_DARK}}}

/* Pestañas más vistosas */
button[data-baseweb="tab"]{{
    font-family:'Segoe UI',sans-serif!important;
    font-size:.93rem!important;font-weight:600!important;color:{C_DARK}!important
}}
button[data-baseweb="tab"][aria-selected="true"]{{
    color:{C_AZUL}!important;border-bottom:3px solid {C_AZUL}!important
}}

/* Tablas con sombra */
.stDataFrame{{border-radius:10px;overflow:hidden;box-shadow:0 2px 10px rgba(0,0,0,.07)}}

/* Métricas */
[data-testid="stMetricValue"]{{font-size:1.4rem!important;font-weight:700!important}}
[data-testid="stMetricLabel"]{{font-size:.82rem!important;color:#555!important}}

/* Botones degradado corporativo */
.stDownloadButton>button,.stButton>button{{
    background:linear-gradient(90deg,{C_DARK},{C_AZUL})!important;
    color:white!important;border:none!important;border-radius:8px!important;
    font-weight:600!important;padding:8px 18px!important;
    box-shadow:0 3px 10px rgba(0,40,85,.25)!important
}}

/* Inputs de sidebar con etiquetas visibles */
.stNumberInput label p{{color:{C_DARK}!important;font-weight:600!important;font-size:.88rem!important}}
</style>""", unsafe_allow_html=True)

# ════════════════════════════════════════════════════════════
# CABECERA
# ════════════════════════════════════════════════════════════
cH1, cH2 = st.columns([5, 1])
with cH1:
    st.title("⚡ Sistema de Cálculo de Indicadores de Continuidad")
    st.markdown("<div class='creator-badge'>👨‍💻 Ing. Cristian Rollano</div>", unsafe_allow_html=True)
with cH2:
    st.markdown("<br>", unsafe_allow_html=True)
    st.info(f"👤 **{st.session_state['usuario_actual']}**")
    if st.button("⏻  Salir"):
        st.session_state['logueado'] = False
        st.rerun()

# ════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════
st.sidebar.markdown(f"""
<div style='background:linear-gradient(135deg,{C_DARK},{C_AZUL});
            border-radius:10px;padding:13px 16px;margin-bottom:14px;
            box-shadow:0 4px 14px rgba(0,40,85,.3)'>
  <p style='color:white;font-size:1.0rem;font-weight:700;margin:0;letter-spacing:.4px'>
    ⚙️ Parámetros del Sistema
  </p>
</div>""", unsafe_allow_html=True)

# Tabla normativa simétrica con HTML puro
st.sidebar.markdown(f"""
<div style='background:white;border-radius:10px;padding:14px;
            box-shadow:0 2px 10px rgba(0,0,0,.08);margin-bottom:14px'>
  <p style='font-size:.8rem;font-weight:700;color:{C_DARK};
            margin:0 0 10px;letter-spacing:.3px;text-transform:uppercase'>
    📋 Límites Normativos Semestrales
  </p>
  <table style='width:100%;border-collapse:collapse;font-size:.82rem;table-layout:fixed'>
    <colgroup>
      <col style='width:44%'>
      <col style='width:28%'>
      <col style='width:28%'>
    </colgroup>
    <thead>
      <tr>
        <th style='background:{C_DARK};color:white;padding:7px 6px;
                   text-align:left;border-radius:5px 0 0 0'>Indicador</th>
        <th style='background:{C_DARK};color:white;padding:7px 0;text-align:center'>Cal. 1</th>
        <th style='background:{C_DARK};color:white;padding:7px 0;
                   text-align:center;border-radius:0 5px 0 0'>Cal. 2</th>
      </tr>
    </thead>
    <tbody>
      <tr style='background:#F0F5FF'>
        <td style='padding:8px 6px;font-weight:600;color:{C_DARK}'>⏱️ Tiempo</td>
        <td style='text-align:center;padding:8px 0;font-weight:700;
                   color:{C_AZUL};font-size:.93rem'>6 h</td>
        <td style='text-align:center;padding:8px 0;font-weight:700;
                   color:{C_VERDE};font-size:.93rem'>12 h</td>
      </tr>
      <tr>
        <td style='padding:8px 6px;font-weight:600;color:{C_DARK}'>🔢 Frecuencia</td>
        <td style='text-align:center;padding:8px 0;font-weight:700;
                   color:{C_AZUL};font-size:.93rem'>7</td>
        <td style='text-align:center;padding:8px 0;font-weight:700;
                   color:{C_VERDE};font-size:.93rem'>14</td>
      </tr>
      <tr style='background:#F6F9FF'>
        <td colspan='3' style='padding:7px 6px;color:#777;font-size:.77rem;font-style:italic'>
          Límite mensual = Semestral ÷ 6
        </td>
      </tr>
    </tbody>
  </table>
</div>""", unsafe_allow_html=True)

# Parámetros en form → NO causan rerun hasta presionar el botón
with st.sidebar.form("params_form"):
    cs_1 = st.number_input("Consumidores Calidad 1", value=12537.667, format="%.3f")
    cs_2 = st.number_input("Consumidores Calidad 2", value=1392.833,  format="%.3f")
    st.form_submit_button("✔  Actualizar Parámetros", use_container_width=True)

# ════════════════════════════════════════════════════════════
# PROCESAMIENTO CACHEADO Y VECTORIZADO
# ════════════════════════════════════════════════════════════
@st.cache_data(show_spinner=False)
def leer_mdb(file_bytes: bytes, tabla: str):
    with tempfile.NamedTemporaryFile(delete=False, suffix='.mdb') as tmp:
        tmp.write(file_bytes)
        path = tmp.name
    try:
        conn = pyodbc.connect(r'DRIVER={Microsoft Access Driver (*.mdb, *.accdb)};DBQ=' + path + ';')
        df   = pd.read_sql(f'SELECT * FROM {tabla}', conn)
        conn.close()
        return df
    except Exception as e:
        st.error(f"Error leyendo {tabla}: {e}")
        return None
    finally:
        if os.path.exists(path): os.remove(path)


@st.cache_data(show_spinner=False)
def procesar_datos(bytes_10: bytes, bytes_11: bytes):
    df10 = leer_mdb(bytes_10, "ST10")
    df11 = leer_mdb(bytes_11, "ST11")
    if df10 is None or df11 is None:
        return None, None, None

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

    df10['START'] = _parse_fecha(df10['FECHA_I'], df10['HORA_I'])
    df11['END']   = _parse_fecha(df11['FECHA_R'], df11['HORA_R'])

    df11_last = df11.groupby('NUMERO', sort=False)['END'].max().reset_index()
    df10      = df10.merge(df11_last, on='NUMERO', how='inner')

    df10['DURACION_H'] = (df10['END'] - df10['START']).dt.total_seconds() / 3600.0
    df10 = df10[df10['DURACION_H'] >= 0].copy()

    df10['ORIGEN_LABEL'] = df10['COD_ORIGEN'].astype(str).str.zfill(2).map(MAPA_ORIGEN).fillna('OTROS')
    df10['TIPO_LABEL']   = np.where(
        df10['COD_CAUSA'].astype(str).str.strip().isin(CAUSAS_PROG), 'PROGRAMADA', 'FORZADA'
    )

    df_sir_10 = df10[(df10['ORIGEN_LABEL'] != 'EXTERNO') & (df10['DURACION_H'] >= 0.05)].copy()
    df_sir_10['MES'] = df_sir_10['START'].dt.to_period('M').astype(str)

    cols_join = ['NUMERO','START','ORIGEN_LABEL','COD_CAUSA','TIPO_LABEL','MES']
    df_sir_11 = df11.merge(df_sir_10[cols_join], on='NUMERO', how='inner')
    df_sir_11['DURACION_PASO'] = (df_sir_11['END'] - df_sir_11['START']).dt.total_seconds() / 3600.0
    df_sir_11 = df_sir_11[df_sir_11['DURACION_PASO'] >= 0].copy()

    for cal, col_r in [(1,'CONS_BT_R_1'), (2,'CONS_BT_R_2')]:
        df_sir_11[f'T_POND_{cal}'] = (
            df_sir_11[col_r] * df_sir_11['DURACION_PASO']
            if col_r in df_sir_11.columns else 0.0
        )
    return df10, df_sir_10, df_sir_11


def to_excel(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Datos')
    return buf.getvalue()


def armar_tabla_sir(df_sir_10, df_sir_11, cs_val, cal):
    FILAS   = ['SUBTRANSMISIÓN','DIST. PRIMARIA','DIST. SECUNDARIA']
    col_f   = f'CONS_BT_{cal}'
    col_t   = f'T_POND_{cal}'

    def _piv(df, col, group):
        if col not in df.columns:
            return pd.DataFrame(0.0, index=FILAS, columns=['PROGRAMADA','FORZADA'])
        p = (df.groupby(group)[col].sum() / cs_val).unstack(fill_value=0)
        for c in ['PROGRAMADA','FORZADA']:
            if c not in p.columns: p[c] = 0.0
        return p.reindex(FILAS, fill_value=0)

    p_f = _piv(df_sir_10, col_f, ['ORIGEN_LABEL','TIPO_LABEL'])
    p_t = _piv(df_sir_11, col_t, ['ORIGEN_LABEL','TIPO_LABEL'])

    out = pd.DataFrame(index=FILAS)
    out['(F) PROGRAMADA'] = p_f['PROGRAMADA']
    out['(F) FORZADA']    = p_f['FORZADA']
    out['(F) TOTAL']      = out['(F) PROGRAMADA'] + out['(F) FORZADA']
    out['(T) PROGRAMADA'] = p_t['PROGRAMADA']
    out['(T) FORZADA']    = p_t['FORZADA']
    out['(T) TOTAL']      = out['(T) PROGRAMADA'] + out['(T) FORZADA']
    out.loc['TOTAL DIST.'] = out.loc[['DIST. PRIMARIA','DIST. SECUNDARIA']].sum()
    return out


def generar_html(res_c1, res_c2):
    return f"""<html><head><meta charset="UTF-8">
    <style>body{{font-family:'Segoe UI',sans-serif;padding:36px;color:#333}}
    h1,h2{{color:#002855}}
    table{{width:100%;border-collapse:collapse;margin-bottom:28px;font-size:13px}}
    th,td{{border:1px solid #ddd;padding:7px 10px;text-align:center}}
    th{{background:#002855;color:white}}
    .footer{{margin-top:40px;font-size:11px;color:#999;text-align:center}}
    </style></head><body>
    <h1>Reporte de Indicadores de Continuidad</h1>
    <h2>Calidad 1</h2>{res_c1.round(4).to_html()}
    <h2>Calidad 2</h2>{res_c2.round(4).to_html()}
    <div class="footer">Ing. Cristian Rollano — Sistema de Indicadores</div>
    <script>window.onload=()=>window.print()</script>
    </body></html>""".encode('utf-8')

# ════════════════════════════════════════════════════════════
# HELPERS GRÁFICAS
# ════════════════════════════════════════════════════════════
_LAYOUT = dict(plot_bgcolor=C_WHITE, paper_bgcolor=C_BG, font=dict(family='Segoe UI'))

def _fig(**kw) -> go.Figure:
    f = go.Figure()
    f.update_layout(**{**_LAYOUT, **kw})
    return f

def _tit(txt, sub=""):
    t = f"<b>{txt}</b>" + (f"<br><sup style='color:#666'>{sub}</sup>" if sub else "")
    return dict(text=t, font=dict(size=16, color=C_DARK), x=0.01, xanchor='left')

def _mes_fmt(m):
    try:
        dt = pd.Period(m,'M').to_timestamp()
        nom = ['Ene','Feb','Mar','Abr','May','Jun',
               'Jul','Ago','Sep','Oct','Nov','Dic'][dt.month-1]
        return f"{nom}<br>{str(dt.year)[2:]}"
    except:
        return m

# ════════════════════════════════════════════════════════════
# IMPACTO POR ORIGEN
# ════════════════════════════════════════════════════════════
def graficar_impacto_origen(df_sir_10, df_sir_11):

    def _barra_h(col_t, titulo, cs_val):
        if col_t not in df_sir_11.columns or df_sir_11[col_t].sum() == 0:
            return None
        agg  = df_sir_11.groupby(['ORIGEN_LABEL','TIPO_LABEL'])[col_t].sum().reset_index()
        agg['V'] = agg[col_t] / cs_val
        ors  = sorted(agg['ORIGEN_LABEL'].unique())
        prog = agg[agg['TIPO_LABEL']=='PROGRAMADA'].set_index('ORIGEN_LABEL')['V'].reindex(ors, fill_value=0)
        forz = agg[agg['TIPO_LABEL']=='FORZADA'   ].set_index('ORIGEN_LABEL')['V'].reindex(ors, fill_value=0)
        tot  = prog + forz

        # Altura proporcional al número de orígenes, mínimo 380
        altura = max(380, len(ors) * 100 + 180)

        fig = _fig(
            barmode='stack',
            title=_tit(titulo, "Tiempo acumulado por tipo de interrupción (h/usuario)"),
            height=altura,
            margin=dict(t=88, b=80, l=10, r=155),
            xaxis=dict(title="Horas / usuario", showgrid=True, gridcolor=C_GRID,
                       tickfont=dict(size=12), zeroline=True, zerolinecolor='#ccc'),
            yaxis=dict(tickfont=dict(size=13, color=C_DARK), showgrid=False, automargin=True),
            legend=dict(orientation='h', x=0.5, xanchor='center', y=-0.22,
                        font=dict(size=13), bgcolor='rgba(255,255,255,0.88)',
                        bordercolor='rgba(180,180,180,0.4)', borderwidth=1),
        )
        fig.add_trace(go.Bar(
            name='Programada', y=ors, x=prog.values, orientation='h',
            marker=dict(color=C_PROG, line=dict(color='white',width=1.5)),
            text=[f"{v:.3f}" if v > 0.001 else "" for v in prog.values],
            textposition='inside', insidetextanchor='middle',
            textfont=dict(color='white', size=12),
            hovertemplate="<b>%{y}</b><br>Programada: %{x:.4f} h<extra></extra>",
        ))
        fig.add_trace(go.Bar(
            name='Forzada', y=ors, x=forz.values, orientation='h',
            marker=dict(color=C_FORZ, line=dict(color='white',width=1.5)),
            text=[f"{v:.3f}" if v > 0.001 else "" for v in forz.values],
            textposition='inside', insidetextanchor='middle',
            textfont=dict(color='white', size=12),
            hovertemplate="<b>%{y}</b><br>Forzada: %{x:.4f} h<extra></extra>",
        ))
        for org, t in zip(ors, tot.values):
            if t > 0:
                fig.add_annotation(x=t, y=org, text=f"<b>{t:.3f}</b>",
                                   showarrow=False, xanchor='left', yanchor='middle',
                                   xshift=10, font=dict(size=12, color='#222'))
        return fig

    def _donut(col_t, titulo, cs_val):
        if col_t not in df_sir_11.columns or df_sir_11[col_t].sum() == 0:
            return None
        agg       = df_sir_11.groupby('TIPO_LABEL')[col_t].sum().reset_index()
        total_val = agg[col_t].sum() / cs_val
        colores   = [C_PROG if t=='PROGRAMADA' else C_FORZ for t in agg['TIPO_LABEL']]
        fig = _fig(
            title=_tit(titulo),
            height=400,
            margin=dict(t=80, b=65, l=20, r=20),
            showlegend=True,
            legend=dict(orientation='h', x=0.5, xanchor='center', y=-0.12, font=dict(size=13)),
        )
        fig.add_trace(go.Pie(
            labels=agg['TIPO_LABEL'], values=agg[col_t], hole=0.54,
            marker=dict(colors=colores, line=dict(color='white', width=3)),
            textinfo='label+percent', textfont=dict(size=13),
            pull=[0.05 if t=='FORZADA' else 0 for t in agg['TIPO_LABEL']],
            hovertemplate="<b>%{label}</b><br>%{value:.3f} h·cons<br>%{percent}<extra></extra>",
        ))
        fig.add_annotation(
            text=f"<b>{total_val:.2f}</b><br><span style='font-size:11px'>h/usuario<br>total</span>",
            x=0.5, y=0.5, showarrow=False, font=dict(size=15, color=C_DARK),
        )
        return fig

    col1, col2 = st.columns(2)
    with col1:
        f = _barra_h('T_POND_1', "Impacto por Origen — Calidad 1", cs_1)
        if f:
            st.plotly_chart(f, use_container_width=True)
        else:
            st.info("Sin datos de impacto para Calidad 1.")

    with col2:
        f = _barra_h('T_POND_2', "Impacto por Origen — Calidad 2", cs_2)
        if f:
            st.plotly_chart(f, use_container_width=True)
        else:
            st.info("Sin datos de impacto para Calidad 2.")

    st.markdown("<div class='sec-header'>📌 Distribución: Programadas vs Forzadas</div>",
                unsafe_allow_html=True)

    col3, col4 = st.columns(2)
    with col3:
        f = _donut('T_POND_1', "Distribución de Tiempo — Calidad 1", cs_1)
        if f:
            st.plotly_chart(f, use_container_width=True)
        else:
            st.info("Sin datos para Calidad 1.")

    with col4:
        f = _donut('T_POND_2', "Distribución de Tiempo — Calidad 2", cs_2)
        if f:
            st.plotly_chart(f, use_container_width=True)
        else:
            st.info("Sin datos para Calidad 2.")


# ════════════════════════════════════════════════════════════
# GRÁFICAS MENSUALES
# ════════════════════════════════════════════════════════════
def graficar_incidencia_mensual(df_sir_10, df_sir_11, cs_1, cs_2):

    st.markdown(f"""
    <div class='norm-box'>
      <b style='color:{C_DARK};font-size:.97rem'>📋 Referencia Normativa Semestral</b>
      <table class='norm-table' style='margin-top:10px'>
        <thead>
          <tr>
            <th style='text-align:left;border-radius:6px 0 0 0'>Indicador</th>
            <th style='color:#90CAF9'>Calidad 1</th>
            <th style='color:#A5D6A7'>Calidad 2</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>⏱️ Tiempo de interrupción</td>
            <td><b style='color:{C_AZUL}'>6 h</b> → 1.000 h/mes</td>
            <td><b style='color:{C_VERDE}'>12 h</b> → 2.000 h/mes</td>
          </tr>
          <tr>
            <td>🔢 Frecuencia de interrupción</td>
            <td><b style='color:{C_AZUL}'>7</b> → 1.167/mes</td>
            <td><b style='color:{C_VERDE}'>14</b> → 2.333/mes</td>
          </tr>
        </tbody>
      </table>
      <p style='margin:10px 0 0;font-size:.83rem;color:#666'>
        Barras en <b style='color:{C_ROJO}'>rojo</b> indican meses que superan el límite normativo. &nbsp;
        La línea <b style='color:{C_AMBER}'>ámbar punteada</b> marca el límite mensual.
      </p>
    </div>""", unsafe_allow_html=True)

    # ── agrupaciones ──────────────────────────────────────────────────────
    def _t_mes(col_t, cs_val, lim):
        if col_t not in df_sir_11.columns:
            return pd.DataFrame()
        df = (df_sir_11.groupby('MES')[col_t].sum() / cs_val).reset_index()
        df.columns = ['MES','VALOR']
        df['supera'] = df['VALOR'] > lim
        return df.sort_values('MES')

    def _f_mes(col_f, cs_val, lim):
        if col_f in df_sir_10.columns:
            df = (df_sir_10.groupby('MES')[col_f].sum() / cs_val).reset_index()
            df.columns = ['MES','VALOR']
        else:
            df = df_sir_10.groupby('MES').size().reset_index(name='VALOR')
            df['VALOR'] = df['VALOR'] / cs_val
        df['supera'] = df['VALOR'] > lim
        return df.sort_values('MES')

    # ── constructor de gráfica mensual ─────────────────────────────────────
    def _bar_mes(df, titulo, y_label, lim_mes, color_ok, unidad):
        if df.empty or df['VALOR'].sum() == 0:
            return None, 0

        meses   = df['MES'].tolist()
        valores = df['VALOR'].tolist()
        supera  = df['supera'].tolist()
        n_inc   = sum(supera)
        colores = [C_ROJO if s else color_ok for s in supera]
        ticks   = [_mes_fmt(m) for m in meses]

        # Altura dinámica: más meses → gráfica más ancha no es posible,
        # pero la altura se adapta y los márgenes garantizan leyenda visible
        HEIGHT   = 560
        MARGIN_B = 190   # suficiente para leyenda de 2 líneas

        fig = _fig(
            title=_tit(titulo),
            height=HEIGHT,
            margin=dict(t=80, b=MARGIN_B, l=78, r=36),
            xaxis=dict(
                tickmode='array',
                tickvals=list(range(len(meses))),
                ticktext=ticks,          # mes + año debajo de cada barra
                tickangle=0,
                tickfont=dict(size=12, color='#333'),
                showgrid=False,
                linecolor='#ccd0da',
                fixedrange=True,
            ),
            yaxis=dict(
                title=dict(text=y_label, font=dict(size=13)),
                showgrid=True, gridcolor=C_GRID,
                tickfont=dict(size=12),
                zeroline=True, zerolinecolor='#ccd0da',
            ),
            bargap=0.28,
            showlegend=False,
        )

        # Barras con índice numérico para respetar tickvals
        fig.add_trace(go.Bar(
            x=list(range(len(meses))),
            y=valores,
            marker=dict(color=colores, line=dict(color='white',width=1.5), opacity=0.93),
            customdata=meses,
            hovertemplate=(
                "<b>%{customdata}</b><br>"
                + y_label + ": <b>%{y:.4f}</b> " + unidad + "<extra></extra>"
            ),
        ))

        # Línea de límite normativo
        fig.add_hline(
            y=lim_mes,
            line=dict(color=C_AMBER, width=2.5, dash='dot'),
            annotation=dict(
                text=f"<b>Límite: {lim_mes:.3f} {unidad}</b>",
                font=dict(size=12, color=C_AMBER),
                bgcolor='white', bordercolor=C_AMBER,
                borderwidth=1, borderpad=5,
                xanchor='right', x=1.0,
            ),
        )

        # ── Leyenda fija como anotación — y=-0.35 dentro del paper ─────────
        # Con MARGIN_B=190 y height=560, este punto cae bien dentro del lienzo
        fig.add_annotation(
            xref='paper', yref='paper',
            x=0.5, y=-0.35,
            xanchor='center', yanchor='top',
            text=(
                f"<span style='color:{color_ok};font-size:15px'>█</span>"
                f"  Dentro del límite"
                f"&nbsp;&nbsp;&nbsp;&nbsp;"
                f"<span style='color:{C_ROJO};font-size:15px'>█</span>"
                f"  Supera el límite"
                f"&nbsp;&nbsp;&nbsp;&nbsp;"
                f"<span style='color:{C_AMBER}'>· · ·</span>"
                f"  Límite mensual ({lim_mes:.3f} {unidad})"
            ),
            showarrow=False,
            font=dict(size=12, color='#444'),
            bgcolor='rgba(255,255,255,0.95)',
            bordercolor='rgba(180,180,180,0.5)',
            borderwidth=1, borderpad=12,
        )

        return fig, n_inc

    # ══════════════════════════
    # TIEMPO MENSUAL
    # ══════════════════════════
    st.markdown("<div class='sec-header'>⏱️ Tiempo de Interrupción Mensual (horas / usuario)</div>",
                unsafe_allow_html=True)

    df_t1 = _t_mes('T_POND_1', cs_1, LIM_T_MES_C1)
    df_t2 = _t_mes('T_POND_2', cs_2, LIM_T_MES_C2)

    col_t1, col_t2 = st.columns(2)
    with col_t1:
        fig, n = _bar_mes(df_t1, "Tiempo Mensual — Calidad 1",
                          "Horas / usuario", LIM_T_MES_C1, C_AZUL, "h")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            if n > 0:
                st.error(f"⚠️ {n} mes(es) superan el límite de {LIM_T_MES_C1:.3f} h/mes — Cal. 1")
            else:
                st.success("✅ Todos los meses dentro del límite — Cal. 1")
        else:
            st.info("Sin datos de tiempo para Calidad 1.")

    with col_t2:
        fig, n = _bar_mes(df_t2, "Tiempo Mensual — Calidad 2",
                          "Horas / usuario", LIM_T_MES_C2, C_VERDE, "h")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            if n > 0:
                st.error(f"⚠️ {n} mes(es) superan el límite de {LIM_T_MES_C2:.3f} h/mes — Cal. 2")
            else:
                st.success("✅ Todos los meses dentro del límite — Cal. 2")
        else:
            st.info("Sin datos de tiempo para Calidad 2.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ══════════════════════════
    # FRECUENCIA MENSUAL
    # ══════════════════════════
    st.markdown("<div class='sec-header'>📊 Frecuencia de Interrupción Mensual (eventos / usuario)</div>",
                unsafe_allow_html=True)

    df_f1 = _f_mes('CONS_BT_1', cs_1, LIM_F_MES_C1)
    df_f2 = _f_mes('CONS_BT_2', cs_2, LIM_F_MES_C2)

    col_f1, col_f2 = st.columns(2)
    with col_f1:
        fig, n = _bar_mes(df_f1, "Frecuencia Mensual — Calidad 1",
                          "Eventos / usuario", LIM_F_MES_C1, C_MORADO, "eventos")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            if n > 0:
                st.error(f"⚠️ {n} mes(es) superan el límite de {LIM_F_MES_C1:.3f} eventos/mes — Cal. 1")
            else:
                st.success("✅ Todos los meses dentro del límite — Cal. 1")
        else:
            st.info("Sin datos de frecuencia para Calidad 1.")

    with col_f2:
        fig, n = _bar_mes(df_f2, "Frecuencia Mensual — Calidad 2",
                          "Eventos / usuario", LIM_F_MES_C2, C_NARANJA, "eventos")
        if fig:
            st.plotly_chart(fig, use_container_width=True)
            if n > 0:
                st.error(f"⚠️ {n} mes(es) superan el límite de {LIM_F_MES_C2:.3f} eventos/mes — Cal. 2")
            else:
                st.success("✅ Todos los meses dentro del límite — Cal. 2")
        else:
            st.info("Sin datos de frecuencia para Calidad 2.")

    st.markdown("<br>", unsafe_allow_html=True)

    # ══════════════════════════
    # RESUMEN SEMESTRAL — 2 gráficas separadas
    # ══════════════════════════
    st.markdown("<div class='sec-header'>📈 Resumen Semestral Acumulado vs Límite Normativo</div>",
                unsafe_allow_html=True)

    def _resumen(acum_t, acum_f, lim_t, lim_f, titulo, c_t, c_f):
        cats  = ['Tiempo de Interrupción', 'Frecuencia de Interrupción']
        acums = [acum_t, acum_f]
        lims  = [lim_t, lim_f]
        unids = ['h', 'eventos']
        cfin  = [C_ROJO if a > l else c for a, l, c in zip(acums, lims, [c_t, c_f])]
        pcts  = [(a/l*100) if l > 0 else 0 for a, l in zip(acums, lims)]

        fig = _fig(
            title=_tit(titulo, "Valor acumulado semestral vs límite normativo"),
            height=480,
            margin=dict(t=90, b=100, l=75, r=36),
            xaxis=dict(tickfont=dict(size=14, color=C_DARK), showgrid=False),
            yaxis=dict(title="Valor acumulado", showgrid=True, gridcolor=C_GRID,
                       tickfont=dict(size=12)),
            showlegend=True,
            legend=dict(orientation='h', x=0.5, xanchor='center', y=-0.18,
                        font=dict(size=13), bgcolor='rgba(255,255,255,0.88)',
                        bordercolor='rgba(180,180,180,0.4)', borderwidth=1),
            bargap=0.44,
        )

        for i, (cat, acum, lim, col, unid, pct) in enumerate(
            zip(cats, acums, lims, cfin, unids, pcts)
        ):
            fig.add_trace(go.Bar(
                x=[cat], y=[acum],
                marker=dict(color=col, line=dict(color='white',width=2), opacity=0.91),
                text=[f"<b>{acum:.3f} {unid}</b><br>({pct:.0f}% del límite)"],
                textposition='outside', textfont=dict(size=12, color='#333'),
                width=0.46,
                hovertemplate=(f"<b>{cat}</b><br>Acumulado: {acum:.4f} {unid}<br>"
                               f"Límite: {lim} {unid}<br>Consumo: {pct:.1f}%<extra></extra>"),
                showlegend=False,
            ))
            # Línea horizontal de límite por barra
            fig.add_shape(type='line', x0=i-0.27, x1=i+0.27,
                          y0=lim, y1=lim, xref='x', yref='y',
                          line=dict(color=C_AMBER, width=3.5))
            estado = "✅ OK" if acum <= lim else "⚠️ EXCEDE"
            fig.add_annotation(
                x=cat, y=lim, yshift=14,
                text=f"Límite: <b>{lim}</b> {unid} &nbsp; {estado}",
                showarrow=False,
                font=dict(size=11, color=C_AMBER if acum <= lim else C_ROJO),
                bgcolor='rgba(255,255,255,0.9)',
                bordercolor=C_AMBER, borderwidth=1, borderpad=5,
            )

        # Traza fantasma para leyenda de la línea ámbar
        fig.add_trace(go.Scatter(
            x=[None], y=[None], mode='lines',
            line=dict(color=C_AMBER, width=3.5),
            name='Límite normativo semestral',
        ))
        return fig

    acum_t1 = df_t1['VALOR'].sum() if not df_t1.empty else 0.0
    acum_t2 = df_t2['VALOR'].sum() if not df_t2.empty else 0.0
    acum_f1 = df_f1['VALOR'].sum() if not df_f1.empty else 0.0
    acum_f2 = df_f2['VALOR'].sum() if not df_f2.empty else 0.0

    colR1, colR2 = st.columns(2)
    with colR1:
        st.plotly_chart(
            _resumen(acum_t1, acum_f1, LIM_T_SEM_C1, LIM_F_SEM_C1,
                     "Resumen Semestral — Calidad 1", C_AZUL, C_MORADO),
            use_container_width=True,
        )
        mA, mB = st.columns(2)
        mA.metric("Tiempo acumulado",    f"{acum_t1:.3f} h",
                  delta=f"{acum_t1-LIM_T_SEM_C1:+.3f} vs límite", delta_color="inverse")
        mB.metric("Frecuencia acumulada", f"{acum_f1:.3f}",
                  delta=f"{acum_f1-LIM_F_SEM_C1:+.3f} vs límite", delta_color="inverse")

    with colR2:
        st.plotly_chart(
            _resumen(acum_t2, acum_f2, LIM_T_SEM_C2, LIM_F_SEM_C2,
                     "Resumen Semestral — Calidad 2", C_VERDE, C_NARANJA),
            use_container_width=True,
        )
        mC, mD = st.columns(2)
        mC.metric("Tiempo acumulado",    f"{acum_t2:.3f} h",
                  delta=f"{acum_t2-LIM_T_SEM_C2:+.3f} vs límite", delta_color="inverse")
        mD.metric("Frecuencia acumulada", f"{acum_f2:.3f}",
                  delta=f"{acum_f2-LIM_F_SEM_C2:+.3f} vs límite", delta_color="inverse")

# ════════════════════════════════════════════════════════════
# CARGA DE ARCHIVOS
# ════════════════════════════════════════════════════════════
st.markdown("<div style='height:6px'></div>", unsafe_allow_html=True)
col_u1, col_u2 = st.columns(2)
f10_up = col_u1.file_uploader("📂  Subir archivo ST10 (.mdb)", type=['mdb'])
f11_up = col_u2.file_uploader("📂  Subir archivo ST11 (.mdb)", type=['mdb'])

if f10_up and f11_up:
    b10 = f10_up.read()
    b11 = f11_up.read()

    with st.spinner("⚙️ Procesando archivos… la primera carga puede tardar unos segundos."):
        df10, df_sir_10, df_sir_11 = procesar_datos(b10, b11)

    if df10 is None:
        st.stop()

    # ══════════════════════════════════════════════════════════
    # INYECCIÓN JS — recuerda y restaura la pestaña activa
    # después de cualquier rerun (incluido form_submit).
    # Funciona leyendo/escribiendo sessionStorage en el browser.
    # ══════════════════════════════════════════════════════════
    import streamlit.components.v1 as components
    components.html("""
    <script>
    (function() {
        // Nombre de las pestañas tal como aparecen en el DOM
        const TAB_KEY = 'st_active_tab_indicadores';

        function getTabButtons() {
            return Array.from(document.querySelectorAll('button[data-baseweb="tab"]'));
        }

        function restoreTab() {
            const saved = sessionStorage.getItem(TAB_KEY);
            if (!saved) return;
            const buttons = getTabButtons();
            const target  = buttons.find(b => b.textContent.trim().startsWith(saved));
            if (target && target.getAttribute('aria-selected') !== 'true') {
                target.click();
            }
        }

        function saveTab() {
            const active = document.querySelector('button[data-baseweb="tab"][aria-selected="true"]');
            if (active) {
                // Guardamos las primeras 4 letras/emoji para identificar la pestaña
                sessionStorage.setItem(TAB_KEY, active.textContent.trim().substring(0, 4));
            }
        }

        // Esperar a que el DOM cargue las pestañas y restaurar
        function waitAndRestore(retries) {
            const buttons = getTabButtons();
            if (buttons.length > 0) {
                restoreTab();
                // Escuchar clics para guardar la pestaña elegida
                buttons.forEach(b => b.addEventListener('click', () => {
                    setTimeout(saveTab, 100);
                }));
            } else if (retries > 0) {
                setTimeout(() => waitAndRestore(retries - 1), 150);
            }
        }

        // Ejecutar al cargar y también cuando Streamlit hace rerun
        window.addEventListener('load', () => waitAndRestore(20));
        // MutationObserver para detectar reruns de Streamlit
        const observer = new MutationObserver(() => waitAndRestore(10));
        observer.observe(document.body, { childList: true, subtree: true });
    })();
    </script>
    """, height=0)

    tab_res, tab_graf, tab_ver, tab_ext, tab_umb = st.tabs([
        "📊  Calidades de Servicio",
        "📈  Gráficas y Métricas",
        "📋  Historial de Eventos",
        "🚫  Origen Externo",
        "⏲️  Análisis por Umbral",
    ])

    # ─── TAB 1 ────────────────────────────────────────────────────────────
    with tab_res:
        res_c1 = armar_tabla_sir(df_sir_10, df_sir_11, cs_1, 1)
        res_c2 = armar_tabla_sir(df_sir_10, df_sir_11, cs_2, 2)

        st.markdown("<div class='sec-header'>📊 Informe de Indicadores de Continuidad</div>",
                    unsafe_allow_html=True)

        _, col_dl = st.columns([4, 1])
        with col_dl:
            st.download_button(
                "📄  Descargar Reporte",
                data=generar_html(res_c1, res_c2),
                file_name="Reporte_Indicadores.html",
                mime="text/html",
                use_container_width=True,
                help="Descargue y abra en el navegador → Ctrl+P → Guardar como PDF",
            )

        st.markdown(f"""
        <div style='background:linear-gradient(90deg,{C_AZUL}1A,{C_AZUL}08);
                    border-left:4px solid {C_AZUL};border-radius:8px;
                    padding:10px 18px;margin:16px 0 8px'>
          <span style='font-size:.95rem;font-weight:700;color:{C_DARK}'>🔹 CALIDAD 1</span>
        </div>""", unsafe_allow_html=True)
        st.dataframe(
            res_c1.style.format("{:.4f}")
                  .set_properties(**{'text-align':'center'})
                  .highlight_max(axis=0, color='#FFCDD2')
                  .set_table_styles([{
                      'selector': 'th',
                      'props': [('background-color', C_DARK),('color','white'),('font-weight','600')]
                  }]),
            use_container_width=True,
        )
        st.markdown(f"""
        <div style='background:linear-gradient(90deg,{C_VERDE}1A,{C_VERDE}08);
                    border-left:4px solid {C_VERDE};border-radius:8px;
                    padding:10px 18px;margin:22px 0 8px'>
          <span style='font-size:.95rem;font-weight:700;color:{C_DARK}'>🔹 CALIDAD 2</span>
        </div>""", unsafe_allow_html=True)
        st.dataframe(
            res_c2.style.format("{:.4f}")
                  .set_properties(**{'text-align':'center'})
                  .highlight_max(axis=0, color='#FFCDD2')
                  .set_table_styles([{
                      'selector': 'th',
                      'props': [('background-color', C_DARK),('color','white'),('font-weight','600')]
                  }]),
            use_container_width=True,
        )

    # ─── TAB 2 ────────────────────────────────────────────────────────────
    with tab_graf:
        st.markdown("<div class='sec-header'>🏗️ Análisis de Impacto por Origen y Tipo de Interrupción</div>",
                    unsafe_allow_html=True)
        graficar_impacto_origen(df_sir_10, df_sir_11)

        st.markdown("<hr style='border:none;border-top:2px solid #CDD5E0;margin:30px 0'>",
                    unsafe_allow_html=True)

        st.markdown("<div class='sec-header'>📅 Incidencia Mensual vs Límites Normativos</div>",
                    unsafe_allow_html=True)
        graficar_incidencia_mensual(df_sir_10, df_sir_11, cs_1, cs_2)

    # ─── TAB 3 ────────────────────────────────────────────────────────────
    with tab_ver:
        st.markdown("<div class='sec-header'>📋 Historial Total de Interrupciones</div>",
                    unsafe_allow_html=True)

        total_ev = len(df10)
        prog_ev  = int((df10['TIPO_LABEL'] == 'PROGRAMADA').sum())
        forz_ev  = total_ev - prog_ev

        m1, m2, m3 = st.columns(3)
        m1.metric("Total de eventos",  f"{total_ev:,}")
        m2.metric("Programadas",        f"{prog_ev:,}")
        m3.metric("Forzadas",           f"{forz_ev:,}")

        st.markdown("<br>", unsafe_allow_html=True)
        cols_show = ['NUMERO','START','END','DURACION_H','ORIGEN_LABEL','TIPO_LABEL']
        cols_ex   = [c for c in cols_show if c in df10.columns]
        st.dataframe(
            df10[cols_ex].sort_values('NUMERO').style.format({'DURACION_H':'{:.3f}'}),
            use_container_width=True,
            height=520,
        )

    # ─── TAB 4 ────────────────────────────────────────────────────────────
    with tab_ext:
        st.markdown("<div class='sec-header'>🚫 Interrupciones de Origen Externo</div>",
                    unsafe_allow_html=True)

        df_ext = df10[df10['ORIGEN_LABEL'] == 'EXTERNO'][
            ['NUMERO','START','END','DURACION_H','COD_ORIGEN']
        ]

        st.metric("Eventos externos registrados", f"{len(df_ext):,}")
        st.markdown("<br>", unsafe_allow_html=True)

        if df_ext.empty:
            st.success("No se registraron interrupciones de origen externo.")
        else:
            st.dataframe(
                df_ext.style.format({'DURACION_H':'{:.3f}'}),
                use_container_width=True,
                height=480,
            )

    # ─── TAB 5 ────────────────────────────────────────────────────────────
    with tab_umb:
        st.markdown("<div class='sec-header'>⏲️ Filtro de Criticidad por Duración</div>",
                    unsafe_allow_html=True)

        st.markdown(f"""
        <div class='norm-box'>
          Ajuste el valor de horas para filtrar los eventos críticos.
          El resultado se actualiza automáticamente al cambiar el valor.
        </div>""", unsafe_allow_html=True)

        # number_input con key = 'h_lim' ligado directamente a session_state.
        # Streamlit sincroniza automáticamente el widget con session_state['h_lim'],
        # de modo que el valor persiste en cada rerun y el JS restaura la pestaña.
        st.number_input(
            "Mostrar fallas con duración mayor a (horas):",
            min_value=0,
            step=1,
            key='h_lim',          # ← ligado a session_state['h_lim'] automáticamente
        )

        h_usar = st.session_state['h_lim']

        df_crit = df10[df10['DURACION_H'] > h_usar][
            ['NUMERO','DURACION_H','ORIGEN_LABEL','TIPO_LABEL','START']
        ]

        c1, c2 = st.columns(2)
        c1.metric("Eventos que superan el umbral", f"{len(df_crit):,}")
        c2.metric("Umbral aplicado", f"{h_usar} h")

        st.markdown("<br>", unsafe_allow_html=True)

        if df_crit.empty:
            st.success(f"✅ No hay interrupciones mayores a {h_usar} horas.")
        else:
            st.error(f"⚠️ Se detectaron **{len(df_crit)}** eventos que superan las {h_usar} horas.")
            st.dataframe(
                df_crit.style.format({'DURACION_H':'{:.3f}'}),
                use_container_width=True,
                height=440,
            )
            st.download_button(
                "📥  Exportar a Excel",
                data=to_excel(df_crit),
                file_name=f'Eventos_Criticos_{h_usar}h.xlsx',
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                use_container_width=True,
            )

else:
    st.markdown(f"""
    <div style='text-align:center;padding:70px 20px 50px'>
      <div style='font-size:4rem;margin-bottom:14px'>📂</div>
      <h3 style='color:{C_DARK};margin-bottom:8px;font-family:Segoe UI'>
        Cargue los archivos ST10 y ST11 para comenzar</h3>
      <p style='color:#777;font-size:.93rem;max-width:480px;margin:0 auto'>
        Use los cargadores de archivo en la parte superior de esta página.<br>
        El procesamiento queda en caché automáticamente tras la primera carga.
      </p>
    </div>""", unsafe_allow_html=True)
