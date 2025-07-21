import streamlit as st
import pandas as pd
from sqlalchemy import text,MetaData, Table
from sqlalchemy.dialects.sqlite import insert as sqlite_insert   # ← SQLite 専用
# Postgres の場合は: from sqlalchemy.dialects.postgresql import insert as pg_insert

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

def latest_date_in_db():
    conn = get_conn()
    row = conn.session.execute(text("SELECT MAX(date) AS d FROM slot_data")).first()
    return row.d

def upsert(df: pd.DataFrame):
    conn   = get_conn()
    engine = conn.session.bind

    # slot_data テーブルメタデータを取得
    meta = MetaData()                                     # bind は付けない
    slot = Table("slot_data", meta, autoload_with=engine) # ← autoload_with で反映

    # SQLite: INSERT OR IGNORE で衝突行はスキップ
    for rec in df.to_dict(orient="records"):
        stmt = (
            sqlite_insert(slot)
            .values(**rec)
            .prefix_with("OR IGNORE")        # ← ココがポイント
        )
        conn.session.execute(stmt)

    conn.session.commit()
