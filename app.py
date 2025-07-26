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

COLUMN_MAP = {
    "メッセ武蔵境": {
        "台番号":"台番号","スタート回数":"スタート回数","累計スタート":"累計スタート",
        "BB回数":"BB回数","RB回数":"RB回数","ART回数":"ART回数","最大持ち玉":"最大持玉",
        "BB確率":"BB確率","RB確率":"RB確率","ART確率":"ART確率","合成確率":"合成確率",
        "前日最終スタート":"前日最終スタート",
    },
    "ジャンジャンマールゴット分倍河原":{
        "台番号":"台番号","累計スタート":"累計スタート","BB回数":"BB回数","RB回数":"RB回数",
        "最大持ち玉":"最大持玉","BB確率":"BB確率","RB確率":"RB確率","合成確率":"合成確率",
        "前日最終スタート":"前日最終スタート","スタート回数":"スタート回数",
    },
    "プレゴ立川":{
        "台番号":"台番号","累計スタート":"累計スタート","BB回数":"BB回数","RB回数":"RB回数",
        "最大差玉":"最大差玉","BB確率":"BB確率","RB確率":"RB確率","合成確率":"合成確率",
        "前日最終スタート":"前日最終スタート","スタート回数":"スタート回数",
    },
}

def list_csv_recursive(folder_id: str):
    all_files, queue = [], [(folder_id, "")]
    while queue:
        fid, cur = queue.pop()
        res = drive.files().list(
            q=f"'{fid}' in parents and trashed=false",
            fields="files(id,name,mimeType)",
            pageSize=1000, supportsAllDrives=True).execute()
        for f in res.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                queue.append((f["id"], f"{cur}/{f['name']}"))
            elif f["name"].lower().endswith(".csv"):
                all_files.append({**f, "path": f"{cur}/{f['name']}"})
    return all_files

def normalize(df_raw: pd.DataFrame, store: str) -> pd.DataFrame:
    df = df_raw.rename(columns=COLUMN_MAP[store])
    prob_cols = ["BB確率", "RB確率", "ART確率", "合成確率"]
    for col in prob_cols:
        if col not in df.columns:
            continue
        ser = df[col].astype(str)
        mask_div = ser.str.contains("/")
        if mask_div.any():
            denom = ser[mask_div].str.split("/", expand=True)[1].astype(float)
            df.loc[mask_div, col] = denom.where(denom != 0, pd.NA).rdiv(1.0).fillna(0)
        num = pd.to_numeric(ser[~mask_div], errors="coerce")
        mask_gt1 = num > 1
        num.loc[mask_gt1] = 1.0 / num.loc[mask_gt1]
        df.loc[~mask_div, col] = num
        df[col] = df[col].astype(float)
    int_cols = [
        "台番号", "累計スタート", "スタート回数",
        "BB回数", "RB回数", "ART回数",
        "最大持ち玉", "最大差玉", "前日最終スタート",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

def ensure_store_table(store: str):
    safe = "slot_" + store.replace(" ", "_")
    meta = sa.MetaData()
    if not eng.dialect.has_table(eng.connect(), safe):
        cols = [sa.Column("date", sa.Date), sa.Column("機種", sa.Text)]
        for col in COLUMN_MAP[store].values():
            cols.append(sa.Column(col, sa.Double, nullable=True))
        cols.append(sa.PrimaryKeyConstraint("date", "機種", "台番号"))
        sa.Table(safe, meta, *cols)
        meta.create_all(eng)
    return sa.Table(safe, meta, autoload_with=eng)

def parse_meta(path: str):
    parts = path.strip("/").split("/")
    if len(parts) < 3:
        raise ValueError(f"path 形式が想定外: {path}")
    store, machine, date = parts[-3], parts[-2], dt.date.fromisoformat(parts[-1][-14:-4])
    return store, machine, date

# ========================= データ取り込み =========================
if mode == "📥 データ取り込み":
    st.header("Google Drive → Postgres インポート")
    folder_id = st.text_input("Google Drive フォルダ ID")
    c1, c2 = st.columns(2)
    imp_start = c1.date_input("開始日", value=dt.date(2025, 1, 1))
    imp_end   = c2.date_input("終了日", value=dt.date.today())

    if st.button("🚀 インポート実行", disabled=not folder_id):
        files = [f for f in list_csv_recursive(folder_id)
                 if imp_start <= parse_meta(f["path"])[2] <= imp_end]
        st.write(f"🔍 対象 CSV: **{len(files)} 件**")
        bar = st.progress(0.0)
        for i, f in enumerate(files, 1):
            raw = drive.files().get_media(fileId=f["id"]).execute()
            df_raw = pd.read_csv(io.BytesIO(raw), encoding="shift_jis", on_bad_lines="skip")
            store, machine, date = parse_meta(f["path"])
            if store not in COLUMN_MAP:
                st.warning(f"マッピング未定義: {store} → スキップ"); continue
            table = ensure_store_table(store)
            df = normalize(df_raw, store)
            df["機種"], df["date"] = machine, date
            df = df[[c for c in df.columns if c in table.c.keys()]]
            if df.empty:
                continue
            stmt = (
                pg_insert(table)
                .values(df.to_dict("records"))
                .on_conflict_do_nothing()
            )
            with eng.begin() as conn:
                conn.execute(stmt)
            bar.progress(i / len(files))
        st.success("インポート完了！")

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

    q_slot = sa.select(tbl.c.台番号).where(
        tbl.c.機種 == machine_sel,
        tbl.c.date.between(vis_start, vis_end)
    ).distinct().order_by(tbl.c.台番号)
    with eng.connect() as conn:
        slots = [r[0] for r in conn.execute(q_slot)]
    if not slots:
        st.warning("この機種のデータがありません"); st.stop()
    slot_sel = st.selectbox("台番号", slots)

    sql = sa.select(tbl).where(
        tbl.c.date.between(vis_start, vis_end),
        tbl.c.機種 == machine_sel,
        tbl.c.台番号 == slot_sel
    ).order_by(tbl.c.date)
    df = pd.read_sql(sql, eng)
    if df.empty:
        st.warning("データがありません"); st.stop()

    # === 表示処理: 常に 1/◯ 表示 ===
    df_plot = df.copy()
    df_plot["台番号"] = df_plot["台番号"].astype("Int64")
    df_plot["plot_val"] = df_plot["合成確率"]

    y_axis = alt.Axis(
        title="1 / 合成確率",
        format=".4f",
        labelExpr='"1/" + format(round(1 / datum.value), "d")'
    )
    tooltip_fmt = ".4f"

    st.subheader(f"📈 合成確率 | {machine_sel} | 台 {slot_sel}")
    chart = alt.Chart(df_plot).mark_line().encode(
        x="date:T",
        y=alt.Y("plot_val:Q", axis=y_axis),
        tooltip=["date", alt.Tooltip("plot_val:Q", title="値", format=tooltip_fmt)]
    ).properties(height=300)
    st.altair_chart(chart, use_container_width=True)
