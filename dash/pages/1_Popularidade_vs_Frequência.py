import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import text
from utils_spotify import get_engine, get_filters

# ===============================
# Configuração da Página
# ===============================
st.set_page_config(page_title="Popularidade vs Frequência", layout="wide")

# ===============================
# Conexão com Banco
# ===============================
engine = get_engine()
params, clause = get_filters()

# ===============================
# Título
# ===============================
st.title("🎶 Popularidade vs Frequência de Reprodução")

# ===============================
# Query para Popularidade vs Frequência
# ===============================
sql_scatter = text(f"""
    SELECT 
        t.popularity,
        COUNT(ph.track_id) AS freq
    FROM playback_history ph
    JOIN track t ON t.track_id = ph.track_id
    JOIN track_artist ta ON ta.track_id = t.track_id
    JOIN artist a ON a.artist_id = ta.artist_id
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause}
    GROUP BY t.popularity
    ORDER BY t.popularity
""")

df_scatter = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_scatter = pd.read_sql(sql_scatter, conn, params=params)

# ===============================
# Query para Horas x Dias (Heatmap)
# ===============================
sql_heatmap = text(f"""
    SELECT 
        EXTRACT(DOW FROM ph.played_at) AS dia_semana,
        EXTRACT(HOUR FROM ph.played_at) AS hora,
        SUM(ph.playback_sec)/60.0 AS minutos
    FROM playback_history ph
    JOIN track t ON t.track_id = ph.track_id
    JOIN track_artist ta ON ta.track_id = t.track_id
    JOIN artist a ON a.artist_id = ta.artist_id
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause}
    GROUP BY dia_semana, hora
    ORDER BY hora, dia_semana
""")

df_heatmap = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_heatmap = pd.read_sql(sql_heatmap, conn, params=params)

# ===============================
# Plot: Scatter Popularidade vs Frequência
# ===============================
if not df_scatter.empty:
    fig_scatter = px.scatter(
        df_scatter,
        x="popularity",
        y="freq",
        color="freq",
        color_continuous_scale="greens",
        size="freq",
        labels={"popularity": "Popularidade da Faixa", "freq": "Frequência de Reprodução"},
        title="Popularidade vs Frequência de Reprodução"
    )
    fig_scatter.update_layout(paper_bgcolor="#121212", plot_bgcolor="#121212", font=dict(color="white"))

# ===============================
# Plot: Heatmap Listening Hours
# ===============================
if not df_heatmap.empty:
    df_pivot = df_heatmap.pivot(index="hora", columns="dia_semana", values="minutos").fillna(0)

    dias = ["Seg", "Ter", "Qua", "Qui", "Sex", "Sab", "Dom"]
    df_pivot.columns = [dias[int(c)] for c in df_pivot.columns]

    fig = make_subplots(
        rows=1, cols=2, 
        column_widths=[0.8, 0.2],
        subplot_titles=("Listening Hours", "Total por Hora"),
        horizontal_spacing=0.05
    )

    # Heatmap
    heatmap = go.Heatmap(
        z=df_pivot.values,
        x=df_pivot.columns,
        y=df_pivot.index,
        colorscale="Greens",
        colorbar=dict(title="Minutos")
    )
    fig.add_trace(heatmap, row=1, col=1)

    # Barras Totais por Hora (com minutos exibidos)
    bar = go.Bar(
        y=df_pivot.index,
        x=df_pivot.sum(axis=1),
        orientation="h",
        marker=dict(color="#1DB954"),
        text=df_pivot.sum(axis=1).astype(int),  # minutos ao lado da barra
        textposition="outside",
        name="Total por Hora"
    )
    fig.add_trace(bar, row=1, col=2)

    fig.update_layout(
        paper_bgcolor="#121212",
        plot_bgcolor="#121212",
        font=dict(color="white"),
        height=600,
        showlegend=False
    )

col1, col2 = st.columns([0.6, 0.4])

with col1:

    st.plotly_chart(fig, use_container_width=True)
with col2:

    st.plotly_chart(fig_scatter, use_container_width=True)
