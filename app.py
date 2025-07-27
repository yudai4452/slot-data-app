import io, datetime as dt, pandas as pd, streamlit as st
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import altair as alt

st.set_page_config(page_title="Slot Manager", layout="wide")
mode = st.sidebar.radio("モード", ("📥 データ取り込み", "📊 可視化"))
st.title("🎰 Slot Data Manager & Visualizer")

SA_INFO = st.secrets["gcp_service_account"]
PG_CFG  = st.secrets["connections"]["slot_db"]

@st.cache_resource
def gdrive():
    creds = Credentials.from_service_account_info(
        SA_INFO, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    return build("drive", "v3", credentials=creds)
drive = gdrive()

@st.cache_resource
def engine():
    url = (f"postgresql+psycopg2://{PG_CFG.username}:{PG_CFG.password}"
           f"@{PG_CFG.host}:{PG_CFG.port}/{PG_CFG.database}?sslmode=require")
    return sa.create_engine(url, pool_pre_ping=True)
eng = engine()

# ========================= 可視化モード =========================
if mode == "📊 可視化":
    st.header("DB 可視化")

    with eng.connect() as conn:
        stores = [r[0] for r in conn.execute(sa.text(
            "SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'"))]
    if not stores:
        st.info("まず取り込みモードでデータを入れてください。")
        st.stop()

    store_sel = st.selectbox("店舗", stores)
    tbl = sa.Table(store_sel, sa.MetaData(), autoload_with=eng)

    c1, c2 = st.columns(2)
    vis_start = c1.date_input("開始日", value=dt.date(2025, 1, 1))
    vis_end   = c2.date_input("終了日", value=dt.date.today())

    q_machine = sa.select(tbl.c.機種).where(tbl.c.date.between(vis_start, vis_end)).distinct()
    with eng.connect() as conn:
        machines = [r[0] for r in conn.execute(q_machine)]
    if not machines:
        st.warning("指定期間にデータがありません"); st.stop()
    machine_sel = st.selectbox("機種", machines)

    sql_all = sa.select(tbl).where(
        tbl.c.date.between(vis_start, vis_end),
        tbl.c.機種 == machine_sel
    ).order_by(tbl.c.date, tbl.c.台番号)
    df_all = pd.read_sql(sql_all, eng)
    if df_all.empty:
        st.warning("データがありません"); st.stop()

    df_all["台番号"] = df_all["台番号"].astype("Int64")
    df_all["plot_val"] = df_all["合成確率"]

    # 合成確率平均を日付単位で算出
    df_avg = df_all.groupby("date", as_index=False)["plot_val"].mean()

    y_axis = alt.Axis(
        title="合成確率の平均",
        format=".4f",
        labelExpr='datum.value == 0 ? "0" : "1/" + format(round(1 / datum.value), "d")'
    )
    tooltip_fmt = ".4f"

    st.subheader(f"📈 合成確率の平均 | {machine_sel}")
    chart_avg = alt.Chart(df_avg).mark_line(strokeWidth=3).encode(
        x=alt.X("date:T", title="日付"),
        y=alt.Y("plot_val:Q", axis=y_axis),
        tooltip=["date", alt.Tooltip("plot_val:Q", title="平均合成確率", format=tooltip_fmt)]
    ).properties(height=800).configure_axis(
        labelFontSize=14,
        titleFontSize=16
    )
    st.altair_chart(chart_avg, use_container_width=True)

    st.subheader(f"📈 台番号別 合成確率 | {machine_sel}")
    slots = sorted(df_all["台番号"].dropna().unique())
    slot_sel = st.selectbox("台番号", slots)
    df_slot = df_all[df_all["台番号"] == slot_sel]
    if df_slot.empty:
        st.warning("データがありません"); st.stop()

    chart_slot = alt.Chart(df_slot).mark_line(strokeWidth=3).encode(
        x=alt.X("date:T", title="日付"),
        y=alt.Y("plot_val:Q", axis=y_axis),
        tooltip=["date", alt.Tooltip("plot_val:Q", title="合成確率", format=tooltip_fmt)]
    ).properties(height=800).configure_axis(
        labelFontSize=14,
        titleFontSize=16
    )
    st.altair_chart(chart_slot, use_container_width=True)

    st.caption(f"全台番号: {', '.join(map(str, slots))}")
