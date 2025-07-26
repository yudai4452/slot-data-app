import io, datetime as dt, pandas as pd, streamlit as st
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import altair as alt

# ---------- Streamlit 基本 ----------
st.set_page_config(page_title="Slot Manager", layout="wide")

# ---------- サイドバー: モード選択 ----------
mode = st.sidebar.radio("モード", ("📥 データ取り込み", "📊 可視化"))

st.title("🎰 Slot Data Manager & Visualizer")

# ---------- Secrets ----------
SA_INFO = st.secrets["gcp_service_account"]
PG_CFG  = st.secrets["connections"]["slot_db"]

# ---------- Google Drive ----------
@st.cache_resource
def gdrive():
    creds = Credentials.from_service_account_info(
        SA_INFO, scopes=["https://www.googleapis.com/auth/drive.readonly"])
    return build("drive", "v3", credentials=creds)
drive = gdrive()

# ---------- Postgres ----------
@st.cache_resource
def engine():
    url = (f"postgresql+psycopg2://{PG_CFG.username}:{PG_CFG.password}"
           f"@{PG_CFG.host}:{PG_CFG.port}/{PG_CFG.database}?sslmode=require")
    return sa.create_engine(url, pool_pre_ping=True)
eng = engine()

# ---------- COLUMN_MAP, list_csv_recursive, normalize, ensure_store_table, parse_meta ----------
# (※ 既存の関数は変更なしでそのまま貼り付け)
# ... <中略: 前バージョンの関数ブロックをそのまま保持> ...

# ========== 取り込みモード ==========
if mode == "📥 データ取り込み":
    st.header("Google Drive → Postgres インポート")
    folder_id = st.text_input("Google Drive フォルダ ID")
    col_s, col_e = st.columns(2)
    import_start = col_s.date_input("取り込み開始日", value=dt.date(2025, 1, 1))
    import_end   = col_e.date_input("取り込み終了日", value=dt.date.today())

    if st.button("🚀 取り込み実行") and folder_id:
        files = [f for f in list_csv_recursive(folder_id)
                 if import_start <= parse_meta(f["path"])[2] <= import_end]
        st.write(f"🔍 対象 CSV: {len(files)} 件")
        bar = st.progress(0.0)
        for i, f in enumerate(files, 1):
            raw = drive.files().get_media(fileId=f["id"]).execute()
            df_raw = pd.read_csv(io.BytesIO(raw), encoding="shift_jis")
            store, machine, date = parse_meta(f["path"])
            if store not in COLUMN_MAP:
                st.warning(f"マッピング未定義: {store} をスキップ"); continue
            table = ensure_store_table(store)
            df = normalize(df_raw, store)
            df["機種"], df["date"] = machine, date
            valid = set(table.c.keys())
            df = df[[c for c in df.columns if c in valid]]
            if df.empty: continue
            stmt = (pg_insert(table).values(df.to_dict("records")).on_conflict_do_nothing())
            with eng.begin() as conn:
                conn.execute(stmt)
            bar.progress(i/len(files))
        st.success("インポート完了！")

# ========== 可視化モード ==========
if mode == "📊 可視化":
    st.header("DB 可視化 & 集計")
    # 店舗リスト取得
    with eng.connect() as conn:
        stores = [r[0] for r in conn.execute(sa.text(
            "SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'"))]

    if not stores:
        st.info("まず取り込みモードでデータを入れてください。")
        st.stop()

    store_sel = st.selectbox("店舗", stores)
    tbl = sa.Table(store_sel, sa.MetaData(), autoload_with=eng)

    # 日付レンジ
    d1, d2 = st.columns(2)
    vis_start = d1.date_input("表示開始日", value=dt.date(2025, 1, 1))
    vis_end   = d2.date_input("表示終了日", value=dt.date.today())

    # データ取得
    sql = sa.select(tbl).where(tbl.c.date.between(vis_start, vis_end))
    df_show = pd.read_sql(sql, eng)

    if df_show.empty:
        st.warning("該当期間にデータがありません")
        st.stop()

    # ---------- 簡易サマリー ----------
    st.subheader("📊 サマリー")
    col_a, col_b = st.columns(2)
    col_a.metric("平均合成確率", f"{df_show['合成確率'].mean():.3%}")
    col_b.metric("総BB回数", int(df_show['BB回数'].sum()))

    # ---------- 折れ線グラフ ----------
    st.subheader("📈 合成確率（台番号別）")
    df_line = df_show.pivot(index="date", columns="台番号", values="合成確率")
    st.line_chart(df_line)

    # ---------- ヒートマップ ----------
    st.subheader("🗺️ 日付×台番号 ヒートマップ（BB回数）")
    heat = alt.Chart(df_show).mark_rect().encode(
        x="date:T",
        y="台番号:O",
        color=alt.Color("BB回数:Q", scale=alt.Scale(scheme="greenblue")),
        tooltip=["date", "台番号", "BB回数"]
    ).properties(height=400)
    st.altair_chart(heat, use_container_width=True)
