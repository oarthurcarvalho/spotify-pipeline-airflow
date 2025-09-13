import streamlit as st

import matplotlib.pyplot as plt
import matplotlib.patches as patches
import matplotlib.image as mpimg
import requests
from io import BytesIO
from datetime import datetime

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import text
from utils_spotify import get_engine, default_page_config, SPOTIFY_BG

# -------------------------------------------
# Configuração inicial
# -------------------------------------------
default_page_config()

st.markdown("""
    <style>
    .card {
        background-color: transparent; /* fundo transparente */
        border-radius: 12px;
        padding: 20px;
        text-align: center;
        margin: 10px;
    }
    .card h1 {
        color: #1DB954;  /* Verde Spotify */
        font-size: 64px; /* aumentei o número */
        margin: 0;
        font-weight: bold;
    }
    .card p {
        color: #FFFFFF;
        font-size: 20px; /* aumentei o texto de descrição */
        margin: 0;
    }
    </style>
""", unsafe_allow_html=True)

st.title("Visão Geral")

# -------------------------------------------
# Conexão com o Banco
# ------------------------------------------
engine = get_engine()


# -------------------------------------------
# Filtros
# -------------------------------------------

sql_artists = text("SELECT DISTINCT name FROM artist ORDER BY name;")
df_artists = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_artists = pd.read_sql(sql_artists, conn)

with st.sidebar:
    st.markdown("### Filtros")
    periodo = st.date_input(
        "Selecione o período",
        value=(pd.to_datetime("2025-01-01"), pd.to_datetime(datetime.today())),
        min_value=pd.to_datetime("2025-01-01"),
        max_value=pd.to_datetime(datetime.today())
    )

    artistas_sel = st.multiselect(
        "Filtrar por artista(s)",
        options=df_artists['name'].tolist(),
        default=[]
    )

if isinstance(periodo, (list, tuple)) and len(periodo) == 2:
    start_date, end_date = periodo
else:
    start_date, end_date = datetime(2025, 1, 1), datetime(2025, 9, 1)

# Params para query
extra = {}
if artistas_sel:
    extra["artistas"] = artistas_sel

params = {
    "ds": str(start_date),
    "de": str(end_date),
    **extra
}

st.session_state['periodo'] = periodo
st.session_state['artistas_sel'] = artistas_sel

# -------------------------------------------
# KPIs
# -------------------------------------------
clause = ""
extra = {}
if artistas_sel:
    clause = "AND a.name = ANY(:artists)"
    extra = {"artists": artistas_sel}

sql_kpi = text(f"""
    SELECT
        COUNT(DISTINCT a.artist_id) AS num_artists,
        COUNT(DISTINCT t.track_id)  AS num_tracks,
        SUM(ph.playback_sec)/3600.0 AS hours
    FROM playback_history ph
    JOIN track t ON t.track_id = ph.track_id
    JOIN track_artist ta ON ta.track_id = t.track_id
    JOIN artist a ON a.artist_id = ta.artist_id
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause}
""")

params = {"ds": str(start_date), "de": str(end_date), **extra}

df_kpi = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_kpi = pd.read_sql(sql_kpi, conn, params=params)

if not df_kpi.empty:

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown(
            f'<div class="card"><h1>{df_kpi["hours"][0]:.1f}</h1><p>Horas de Escuta</p></div>',
            unsafe_allow_html=True
        )

    with col2:
        st.markdown(
            f'<div class="card"><h1>{int(df_kpi["num_artists"][0])}</h1><p>Artistas Únicos</p></div>',
            unsafe_allow_html=True
        )

    with col3:
        st.markdown(
            f'<div class="card"><h1>{int(df_kpi["num_tracks"][0])}</h1><p>Faixas Únicas</p></div>',
            unsafe_allow_html=True
        )

# -------------------------------------------
# Músicas Skipadas
# -------------------------------------------

sql_skip = text(f"""
    SELECT 
        SUM(CASE WHEN was_played = FALSE THEN 1 ELSE 0 END) AS skipadas,
        SUM(CASE WHEN was_played = TRUE THEN 1 ELSE 0 END) AS completadas
    FROM playback_history ph
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause if clause else ""}
""")

df_skip = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_skip = pd.read_sql(sql_skip, conn, params=params)

if not df_skip.empty:
    df_donut = df_skip.melt(var_name="status", value_name="qtd")
    fig_donut = px.pie(
        df_donut, values="qtd", names="status",
        hole=0.5,  # Donut
        color="status",
        color_discrete_map={
            "completadas": "#1DB954",  # Verde Spotify
            "skipadas": "#1f77b4"      # Azul claro
        }
    )
    fig_donut.update_traces(textposition="inside", textinfo="percent+label")
    fig_donut.update_layout(
        plot_bgcolor=SPOTIFY_BG,
        paper_bgcolor=SPOTIFY_BG,
        font=dict(color="white"),
        margin=dict(l=10, r=10, t=40, b=10),
        height=400   # mesmo valor do Listening Time
    )
# -------------------------------------------
# Tendência mensal
# -------------------------------------------
sql_trend = text(f"""
    SELECT ph.played_at::date as dia,
       SUM(ph.playback_sec)/3600.0 AS horas
    FROM playback_history ph
        JOIN track t ON t.track_id = ph.track_id
        JOIN track_artist ta ON ta.track_id = t.track_id
        JOIN artist a ON a.artist_id = ta.artist_id
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause}
    GROUP BY dia
    ORDER BY dia;
""")

df_trend = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_trend = pd.read_sql(sql_trend, conn, params=params)

if not df_trend.empty:
    st.subheader("⏱️ Horas de Escuta por Dia")

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=df_trend["dia"],
        y=df_trend["horas"],
        mode="lines",
        line=dict(color="#1DB954", width=3),
        fill="tozeroy",
        fillcolor="rgba(29,185,84,0.4)",  # verde translúcido
        hovertemplate="%{y:.1f} horas<br>%{x|%d %b %Y}<extra></extra>"
    ))

    fig.update_layout(
        xaxis=dict(showgrid=False, zeroline=False, color="#aaa"),
        yaxis=dict(showgrid=False, zeroline=False, color="#aaa"),
        plot_bgcolor=SPOTIFY_BG,
        paper_bgcolor=SPOTIFY_BG,
        margin=dict(l=20, r=20, t=40, b=20),
        font=dict(color="white", size=14),
        title=dict(text="Listening Time", font=dict(size=20, color="white"))
    )

xcol1, xcol2 = st.columns([0.7, 0.3])

with xcol1:
    st.plotly_chart(fig, use_container_width=True)

with xcol2:
    st.plotly_chart(fig_donut, use_container_width=True)

# -------------------------------------------
# Top 6 Artistas (grid estilo Spotify Wrapped)
# -------------------------------------------
sql_top_artists = text(f"""
    SELECT a.name,
       COALESCE(a.image_url, 'https://via.placeholder.com/120') AS image_url,
       SUM(ph.playback_sec)/60.0 AS minutes
    FROM playback_history ph
        JOIN track t ON t.track_id = ph.track_id
        JOIN track_artist ta ON ta.track_id = t.track_id
        JOIN artist a ON a.artist_id = ta.artist_id
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause}
    GROUP BY a.name, a.image_url
    ORDER BY minutes DESC
    LIMIT 6;
""")

df_top_artists = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_top_artists = pd.read_sql(sql_top_artists, conn, params=params)

def render_top_artists(df_top):
    # --- Ajustes que você pode mexer ---
    img_size = 0.45      # tamanho do círculo da imagem (aumenta/diminui a foto)
    rank_font = 110       # tamanho da fonte do número atrás
    rank_x_offset = -0.11 # deslocamento horizontal do número (menor = mais pra esquerda)
    rank_y_offset = 0.55 # deslocamento vertical do número
    name_font = 18       # tamanho da fonte do nome
    minutes_font = 20    # tamanho da fonte dos minutos
    top_margin = 0.15    # quanto o grid desce pra alinhar com o gráfico ao lado
    wspace_val = 0.4     # espaço horizontal entre colunas
    hspace_val = 0.6     # espaço vertical entre linhas
    # -----------------------------------

    fig, axes = plt.subplots(2, 3, figsize=(12, 6), facecolor="none")
    axes = axes.flatten()

    for i, (idx, row) in enumerate(df_top.iterrows()):
        ax = axes[i]
        ax.axis("off")
        ax.set_facecolor("none")

        # Dados do artista
        rank = i + 1
        name = row["name"]
        minutes = int(row["minutes"])
        img_url = row["image_url"] or "https://via.placeholder.com/120"

        # Baixar imagem
        response = requests.get(img_url)
        img = mpimg.imread(BytesIO(response.content), format="jpg")

        # Número grande no fundo
        ax.text(rank_x_offset, rank_y_offset, str(rank),
                fontsize=rank_font, fontweight="bold",
                color="#1DB954", alpha=0.25,
                ha="center", va="center",
                transform=ax.transAxes, zorder=0)

        # Foto circular
        circ = patches.Circle((0.5, 0.55), img_size, transform=ax.transAxes)
        ax.imshow(img, extent=[0.5-img_size, 0.5+img_size,
                               0.55-img_size, 0.55+img_size],
                  clip_path=circ, zorder=1)

        # Nome do artista
        ax.text(0.5, -0.07, name,
                fontsize=name_font, fontweight="bold",
                color="white", ha="center", va="center",
                transform=ax.transAxes, zorder=2)

        # Minutos
        ax.text(0.5, -0.25, f"{minutes} min",
                fontsize=minutes_font, fontweight="bold",
                color="#1DB954", ha="center", va="center",
                transform=ax.transAxes, zorder=2)

    # Ajustes de espaçamento do grid
    plt.subplots_adjust(top=1-top_margin, wspace=wspace_val, hspace=hspace_val)
    fig.patch.set_alpha(0)

    # Título igual ao Top 5 Tracks
    fig.suptitle("🎤 Top 6 Artistas", fontsize=16, fontweight="bold", color="white", x=0.25)

    st.pyplot(fig, transparent=True)

# -------------------------------------------
# Top 5 Tracks (estilo Spotify Wrapped)
# -------------------------------------------
sql_top_tracks = text(f"""
    SELECT t.name, SUM(ph.playback_sec)/60.0 AS minutes
    FROM playback_history ph
    JOIN track t ON t.track_id = ph.track_id
    JOIN track_artist ta ON ta.track_id = t.track_id
    JOIN artist a ON a.artist_id = ta.artist_id
    WHERE ph.played_at::date BETWEEN :ds AND :de
    {clause}
    GROUP BY t.name ORDER BY minutes DESC LIMIT 5
""")

df_top_tracks = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_top_tracks = pd.read_sql(sql_top_tracks, conn, params=params)

def render_top_tracks(df_top):
    st.subheader("🎵 Top 5 Tracks")

    st.markdown("""
        <style>
        .track-row {
            display: flex;
            align-items: center;
            margin: 18px 0;
            font-family: Arial, sans-serif;
        }
        .track-rank {
            font-weight: bold;
            font-size: 18px;
            color: #FFD54F;
            min-width: 35px;
            text-align: right;
            margin-right: 20px;
        }
        .track-content {
            flex: 1;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        .track-name {
            font-weight: 700;
            font-size: 15px;
            color: white;
            margin-bottom: 6px;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .track-bar-container {
            display: flex;
            align-items: center;
        }
        .track-bar-bg {
            flex: 1;
            height: 18px;
            border-radius: 9px;
            background-color: #333;
            margin-right: 10px;
            position: relative;
        }
        .track-bar-fill {
            height: 100%;
            border-radius: 9px;
            background-color: #FFD54F;
        }
        .track-minutes {
            font-weight: 600;
            font-size: 15px;
            color: #FFD54F;
            min-width: 60px;
            text-align: left;
        }
        </style>
    """, unsafe_allow_html=True)

    max_minutes = df_top["minutes"].max()

    for i, row in df_top.iterrows():
        width_pct = int((row["minutes"] / max_minutes) * 100)
        st.markdown(f"""
            <div class="track-row">
                <div class="track-rank">{i+1:02d}</div>
                <div class="track-content">
                    <div class="track-name">{row['name']}</div>
                    <div class="track-bar-container">
                        <div class="track-bar-bg">
                            <div class="track-bar-fill" style="width:{width_pct}%"></div>
                        </div>
                        <div class="track-minutes">{int(row['minutes'])} min</div>
                    </div>
                </div>
            </div>
        """, unsafe_allow_html=True)

col1, col2 = st.columns(2)

with col1:
    if not df_top_artists.empty:
        render_top_artists(df_top_artists)

with col2:
    if not df_top_tracks.empty:
        render_top_tracks(df_top_tracks)
