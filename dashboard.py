import os

import pandas as pd
import psycopg
import streamlit as st
from dotenv import load_dotenv

load_dotenv()

st.set_page_config(page_title="Premier League Warehouse", layout="wide")


def query(sql: str, params: tuple = ()) -> pd.DataFrame:
    """Run a read-only query as the dashboard role and return a DataFrame."""
    with psycopg.connect(
        host=os.getenv("PGHOST", "127.0.0.1"),
        port=os.getenv("PGPORT", "5432"),
        dbname=os.environ["POSTGRES_DB"],
        user="dashboard",
        password=os.environ["DASHBOARD_PASSWORD"],
    ) as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        cols = [c.name for c in cur.description]
        return pd.DataFrame(cur.fetchall(), columns=cols)


@st.cache_data(ttl=300)
def get_seasons() -> list[int]:
    df = query("select distinct season from serving.league_table order by season desc")
    return df["season"].tolist()


@st.cache_data(ttl=300)
def get_table(season: int) -> pd.DataFrame:
    return query(
        """
        select position, team_name as team, played as p, wins as w, draws as d,
               losses as l, goals_for as gf, goals_against as ga,
               goal_difference as gd, points as pts
        from serving.league_table
        where season = %s
        order by position
        """,
        (season,),
    )


@st.cache_data(ttl=300)
def get_results(season: int, team: str) -> pd.DataFrame:
    return query(
        """
        select f.kickoff_utc::date as date, f.matchday, f.venue,
               o.team_name as opponent, f.goals_for as gf,
               f.goals_against as ga, f.result
        from serving.fact_team_matches f
        join serving.fact_team_matches o
          on o.match_id = f.match_id and o.team_id = f.opponent_id
        where f.season = %s and f.team_name = %s
        order by f.kickoff_utc desc
        limit 10
        """,
        (season, team),
    )


@st.cache_data(ttl=300)
def get_latest_result(season: int):
    df = query(
        "select max(kickoff_utc)::date as d from serving.fact_team_matches where season = %s",
        (season,),
    )
    return df["d"].iloc[0]


season = st.sidebar.selectbox(
    "Season",
    get_seasons(),
    format_func=lambda s: f"{s}/{str(s + 1)[-2:]}",
)

st.title(f"Premier League {season}/{str(season + 1)[-2:]}")
st.caption(f"Latest result in the data: {get_latest_result(season)}")

table = get_table(season)
st.subheader("League table")
st.dataframe(table, hide_index=True)

st.subheader("Points")
st.bar_chart(table.set_index("team")["pts"])

st.subheader("Recent results")
team = st.selectbox("Team", table["team"])
st.dataframe(get_results(season, team), hide_index=True)
