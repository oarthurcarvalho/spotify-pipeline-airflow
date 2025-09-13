import streamlit as st
import pandas as pd
from sqlalchemy import text
from utils_spotify import get_engine, get_filters

st.set_page_config(page_title="Análise do Artista", layout="wide")

SPOTIFY_BG = "#121212"
SPOTIFY_GREEN = "#1DB954"

st.title("🎤 Análise do Artista")

# ===============================
# Filtros globais (apenas período no sidebar)
# ===============================
params, clause = get_filters()
engine = get_engine()

# ===============================
# Dropdown de artista
# ===============================
sql_artists = text("SELECT DISTINCT name FROM artist ORDER BY name;")
df_artists = pd.DataFrame()
if engine:
    with engine.begin() as conn:
        df_artists = pd.read_sql(sql_artists, conn)

artista_sel = st.sidebar.selectbox(
    "Escolha um artista",
    df_artists["name"].tolist() if not df_artists.empty else []
)

if artista_sel and engine:
    params_local = params.copy()
    params_local["artista"] = artista_sel

    # ===============================
    # Pegar foto do artista
    # ===============================
    sql_img = text("""
        SELECT image_url 
        FROM artist 
        WHERE name = :artista
        LIMIT 1;
    """)
    img_url = None
    with engine.begin() as conn:
        res = conn.execute(sql_img, {"artista": artista_sel}).fetchone()
        if res:
            img_url = res[0]

    # Mostrar foto do artista ao lado do nome
    col1, col2 = st.columns([1, 4])
    with col1:
        if img_url:
            st.image(img_url, width=150)
    with col2:
        st.subheader(f"{artista_sel}")

    # ===============================
    # KPIs do artista
    # ===============================
    sql_kpi = text(f"""
        SELECT 
            AVG(t.popularity) AS popularidade_media,
            SUM(ph.playback_sec)/60.0 AS minutos_totais,
            COUNT(DISTINCT t.track_id) AS musicas_diferentes
        FROM playback_history ph
        JOIN track t ON t.track_id = ph.track_id
        JOIN track_artist ta ON ta.track_id = t.track_id
        JOIN artist a ON a.artist_id = ta.artist_id
        WHERE a.name = :artista
          AND ph.played_at::date BETWEEN :ds AND :de
        {clause if clause else ""}
    """)

    df_kpi = pd.DataFrame()
    with engine.begin() as conn:
        df_kpi = pd.read_sql(sql_kpi, conn, params=params_local)

    if not df_kpi.empty:
        col1, col2, col3 = st.columns(3)
        col1.metric("Popularidade Média", f"{df_kpi['popularidade_media'][0]:.1f}")
        col2.metric("Minutos Totais", f"{df_kpi['minutos_totais'][0]:.1f}")
        col3.metric("Músicas Diferentes", int(df_kpi['musicas_diferentes'][0]))

    # ===============================
    # Últimas músicas reproduzidas
    # ===============================
    sql_tracks = text(f"""
        SELECT 
            t.name AS faixa,
            ph.played_at,
            t.popularity
        FROM playback_history ph
        JOIN track t ON t.track_id = ph.track_id
        JOIN track_artist ta ON ta.track_id = t.track_id
        JOIN artist a ON a.artist_id = ta.artist_id
        WHERE a.name = :artista
          AND ph.played_at::date BETWEEN :ds AND :de
        {clause if clause else ""}
        ORDER BY ph.played_at DESC
        LIMIT 20
    """)

    df_tracks = pd.DataFrame()
    with engine.begin() as conn:
        df_tracks = pd.read_sql(sql_tracks, conn, params=params_local)

    st.subheader("Últimas Músicas Reproduzidas")
    if not df_tracks.empty:
        st.dataframe(df_tracks, use_container_width=True, hide_index=True)
    else:
        st.info("Nenhuma reprodução encontrada para esse artista no período selecionado.")

