import streamlit as st
import pandas as pd
from src.db import get_conn

st.set_page_config(page_title="Slot Data Visualizer", layout="wide")
st.title("📊 Slot Data Visualizer")

conn = get_conn()

# 店舗 & 機種セレクタ
stores = conn.query("SELECT DISTINCT store FROM slot_data")["store"].tolist()
store = st.sidebar.selectbox("店舗", stores)

machines = conn.query(
    "SELECT DISTINCT machine FROM slot_data WHERE store = :s",
    params={"s": store})["machine"].tolist()
machine = st.sidebar.selectbox("機種", machines)

metric = st.sidebar.selectbox("見る項目", ["合成確率","BB確率","RB確率"])

sql = """
SELECT date, 台番号, {metric}
  FROM slot_data
 WHERE store   = :store
   AND machine = :machine
 ORDER BY date;
""".format(metric=metric)
df = conn.query(sql, params=dict(store=store, machine=machine))

if df.empty:
    st.info("データがありません")
else:
    pivot = df.pivot(index="date", columns="台番号", values=metric)
    st.line_chart(pivot)           # 日付×台番号ヒートマップなら st.dataframe も可
