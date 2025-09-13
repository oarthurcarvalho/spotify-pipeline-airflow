import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.sql import bindparam

SPOTIFY_BG = "#0B0F14"
SPOTIFY_GREEN = "#1DB954"

@st.cache_resource(show_spinner=False)
def get_engine():
    pg = st.secrets.get("pg", {})
    url = pg.get("url")
    if url:
        return create_engine(url, pool_pre_ping=True)

    host = pg.get("host", "localhost")
    db = pg.get("dbname", "postgres")
    user = pg.get("user", "postgres")
    pwd = pg.get("password", "")
    port = int(pg.get("port", 5432))
    ssl = pg.get("sslmode", "require")

    return create_engine(
        f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}?sslmode={ssl}",
        pool_pre_ping=True
    )

@st.cache_data(ttl=300, show_spinner=False)
def load_artists(_engine, start_date, end_date):
    sql = text("""
        SELECT DISTINCT a.name AS artist_name
        FROM playback_history ph
            JOIN track t ON t.track_id = ph.track_id
            JOIN track_artist ta ON ta.track_id = t.track_id
            JOIN artist a ON a.artist_id = ta.artist_id
        WHERE ph.played_at::date BETWEEN :ds AND :de
        ORDER BY a.name
        LIMIT 2000;
    """)
    with _engine.begin() as conn:
        df = pd.read_sql(sql, conn, params={"ds": str(start_date), "de": str(end_date)})
    return df["artist_name"].tolist()

def artist_clause_and_params(artists):
    """Retorn (clause, bind) para IN :artists quando houver filtro"""
    if artists:
        return " AND a.name IN :artists", {"artists": tuple(artists)}
    return ("", {})

def default_page_config():
    st.set_page_config(page_title="Spotify Wrapped", page_icon="🎧", layout="wide")

def get_filters():
    """
    Cria filtros globais (sidebar) para período e artistas.
    Retorna os parâmetros e o trecho de cláusula SQL correspondente.
    """

    # Sidebar - filtros
    st.sidebar.header("Filtros")

    # Período
    date_range = st.sidebar.date_input(
        "Selecione o período",
        value=(pd.to_datetime("2025-01-01"), pd.to_datetime("2025-09-01")),
        min_value=pd.to_datetime("2025-01-01"),
        max_value=pd.to_datetime("2025-12-31")
    )

    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date, end_date = pd.to_datetime("2025-01-01"), pd.to_datetime("2025-09-01")

    # Lista de artistas (pegando direto do banco pode ser melhor, mas por enquanto fixo)
    # Você pode popular isso via query SELECT name FROM artist ORDER BY name
    artist_list = st.sidebar.multiselect("Filtrar por artista(s)", options=[])

    # Monta os parâmetros pro SQL
    params = {"ds": str(start_date), "de": str(end_date)}
    clause = ""
    if artist_list:
        clause = "AND a.name = ANY(:artists)"
        params["artists"] = artist_list

    return params, clause

