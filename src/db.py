import streamlit as st
import pandas as pd
from sqlalchemy import text,MetaData, Table
from sqlalchemy.dialects.sqlite import insert as sqlite_insert   # ← SQLite 専用
# Postgres の場合は: from sqlalchemy.dialects.postgresql import insert as pg_insert

@st.cache_resource
def get_conn():
    return st.connection("slot_db", type="sql")

def init_db():
    schema = """
    CREATE TABLE IF NOT EXISTS slot_data (
        store TEXT,
        machine TEXT,
        date DATE,
        "台番号" INT,
        "累計スタート" INT,
        "スタート回数" INT,
        "BB回数" INT,
        "RB回数" INT,
        "ART回数" INT,
        "最大持玉" INT,
        "最大差玉" INT,
        "BB確率" DOUBLE PRECISION,
        "RB確率" DOUBLE PRECISION,
        "ART確率" DOUBLE PRECISION,
        "合成確率" DOUBLE PRECISION,
        "前日最終スタート" INT,
        PRIMARY KEY (store, machine, date, "台番号")
    );
    """
    conn = get_conn()
    conn.session.execute(sa.text(schema))
    conn.session.commit()

def upsert(df: pd.DataFrame):
    conn   = get_conn()
    engine = conn.session.bind
    meta   = sa.MetaData()
    slot   = sa.Table("slot_data", meta, autoload_with=engine)
    valid  = set(slot.c.keys())

    df = df[[c for c in df.columns if c in valid]]
    if df.empty:
        return

    stmt = (
        pg_insert(slot)
        .values(df.to_dict("records"))
        .on_conflict_do_nothing(
            index_elements=["store", "machine", "date", "台番号"]
        )
    )
    conn.session.execute(stmt)
    conn.session.commit()

def latest_date_in_db():
    conn = get_conn()
    row  = conn.session.execute(sa.text("SELECT MAX(date) FROM slot_data")).first()
    return row[0]
