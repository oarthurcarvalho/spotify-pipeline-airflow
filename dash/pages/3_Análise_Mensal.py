import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import text
from utils_spotify import get_engine, get_filters

st.set_page_config(page_title="Análise Mensal", layout="wide")

SPOTIFY_BG = "#121212"
SPOTIFY_GREEN = "#1DB954"

st.title("📅 Análise Mensal")

# ===============================
# Filtros globais (sidebar)
# ===============================
params, clause = get_filters()
engine = get_engine()

# ===============================
# Query: minutos por mês
# ===============================
sql_month = text(f"""
    SELECT 
        DATE_TRUNC('month', ph.played_at)::date AS mes,
        SUM(ph.playback_sec)/60.0 AS minutos
    FROM playback_history ph
    JOIN track t ON t.track_id = ph.track_id
    JOIN track_artist ta ON ta.track_id = t.track_id
    JOIN artist a ON a.artist_id = ta.artist_id
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause if clause else ""}
    GROUP BY mes
    ORDER BY mes
""")

df_month = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_month = pd.read_sql(sql_month, conn, params=params)

# ===============================
# Preparar dados
# ===============================
if not df_month.empty:
    df_month["mes"] = pd.to_datetime(df_month["mes"])
    df_month["mes_num"] = df_month["mes"].dt.month
    df_month["mes_nome"] = df_month["mes"].dt.strftime("%b")
    df_month["nivel"] = 1  # <- coluna fixa para alinhar as bolhas

    # ===============================
    # Gráfico de bolhas com minutos no centro
    # ===============================
    fig = px.scatter(
        df_month,
        x="mes_num",
        y="nivel",
        size="minutos",
        color="minutos",
        hover_name="mes_nome",
        hover_data={"mes_num": False, "nivel": False, "minutos": ":.0f"},
        size_max=120,
        color_continuous_scale=[[0, SPOTIFY_GREEN], [1, SPOTIFY_GREEN]],
        text=df_month["minutos"].round(0).astype(int).astype(str) + " min"  # texto dentro da bolha
    )

    fig.update_traces(
        marker=dict(opacity=0.6, line=dict(width=1, color="white")),
        textposition="middle center",
        textfont=dict(size=12, color="white")
    )

    fig.update_layout(
        paper_bgcolor=SPOTIFY_BG,
        plot_bgcolor=SPOTIFY_BG,
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(1, 13)),
            ticktext=["Jan","Fev","Mar","Abr","Mai","Jun","Jul","Ago","Set","Out","Nov","Dez"],
            title="Mês",
            color="white"
        ),
        yaxis=dict(showticklabels=False, showgrid=False, zeroline=False),
        coloraxis_showscale=False,
        showlegend=False,
        height=500
    )

    st.plotly_chart(fig, use_container_width=True)

else:
    st.info("Nenhum dado disponível para o período selecionado.")

