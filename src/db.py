import streamlit as st
import pandas as pd
from sqlalchemy import text

@st.cache_resource
def get_conn():
    """
    Streamlit の SQLConnection ラッパ。
    secrets.toml ➜ [connections.slot_db] で URL を指定。
    """
    return st.connection("slot_db", type="sql")

def init_db():
    """
    アプリ初回起動時に一度だけ呼び出し。
    """
    schema = """
    CREATE TABLE IF NOT EXISTS slot_data (
        store TEXT,
        machine TEXT,
        date DATE,
        台番号 INT,
        累計スタート INT,
        スタート回数 INT,
        BB回数 INT,
        RB回数 INT,
        ART回数 INT,
        最大持玉 INT,
        最大差玉 INT,
        BB確率 REAL,
        RB確率 REAL,
        ART確率 REAL,
        合成確率 REAL,
        前日最終スタート INT,
        PRIMARY KEY (store, machine, date, 台番号)
    );
    """
    conn = get_conn()
    conn.session.execute(text(schema))
    conn.session.commit()

def upsert(df: pd.DataFrame):
    """
    SQLite の場合は to_sql(append)+PRIMARY KEY衝突無視で OK。
    Postgres に替える場合は ON CONFLICT DO UPDATE を使う。
    """
    conn = get_conn()
    df.to_sql("slot_data", con=conn.session.bind,
              if_exists="append", index=False, method="multi")
