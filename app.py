import io, datetime as dt, pandas as pd, streamlit as st
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ---------- Streamlit 基本 ----------
st.set_page_config(page_title="Slot Manager", layout="wide")
st.title("🎰 Slot Data Manager")

# ---------- Secrets ----------
SA_INFO = st.secrets["gcp_service_account"]
PG_CFG  = st.secrets["connections"]["slot_db"]

# ---------- Google Drive ----------
@st.cache_resource
def gdrive():
    creds = Credentials.from_service_account_info(
        SA_INFO, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)
drive = gdrive()

# ---------- Postgres ----------
@st.cache_resource
def engine():
    url = (f"postgresql+psycopg2://{PG_CFG.username}:{PG_CFG.password}"
           f"@{PG_CFG.host}:{PG_CFG.port}/{PG_CFG.database}?sslmode=require")
    return sa.create_engine(url, pool_pre_ping=True)
eng = engine()

# ---------- 店舗ごとの列名マッピング ----------
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

# ---------- UTILS ----------
def list_csv_recursive(folder_id: str):
    """サブフォルダを含め .csv を列挙し 'path' を付ける"""
    all_files, queue = [], [(folder_id, "")]          # (folder_id, 現在のパス)
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
                all_files.append({**f, "path": f"{cur}/{f['name']}"})  # ★ここ★
    return all_files


def normalize(df_raw: pd.DataFrame, store: str) -> pd.DataFrame:
    # ① 列名を共通化
    df = df_raw.rename(columns=COLUMN_MAP[store])

    # ② “1/300” 形式 → 浮動小数 (1 ÷ 300)
    prob_cols = ["BB確率", "RB確率", "ART確率", "合成確率"]
    for col in prob_cols:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                      .str.extract(r"(\d+\.?\d*)")        # 300 を取り出す
                      .astype(float)
                      .rdiv(1)                            # 1 / 300
            )

    # ③ 整数列を Int64 型に
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
        cols = [
            sa.Column("date", sa.Date),
            sa.Column("機種", sa.Text),
        ]
        for col in COLUMN_MAP[store].values():
            cols.append(sa.Column(col, sa.Double, nullable=True))
        cols.append(sa.PrimaryKeyConstraint("date", "機種", "台番号"))
        sa.Table(safe, meta, *cols)
        meta.create_all(eng)
    return sa.Table(safe, meta, autoload_with=eng)

def parse_meta(path: str):
    # 例: データ/メッセ武蔵境/マイジャグラーV/slot_machine_data_2025-07-19.csv
    parts = path.strip("/").split("/")
    if len(parts) < 3:
        raise ValueError(f"path 形式が想定外: {path}")
    store, machine = parts[-3], parts[-2]
    date = dt.date.fromisoformat(parts[-1][-14:-4])
    return store, machine, date

# ---------- UI ----------
folder_id = st.text_input("Google Drive フォルダ ID")
if st.button("🚀 取り込み") and folder_id:
    files = list_csv_recursive(folder_id)
    st.write(f"🔍 見つかった CSV: {len(files)} 件")
    bar = st.progress(0.0)
    for i, f in enumerate(files, 1):
        st.write(f.get("path"), f)         # ← 取り込む CSV の相対パスを表示
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
        stmt = (
            pg_insert(table)
            .values(df.to_dict("records"))
            .on_conflict_do_nothing()
        )
        with eng.begin() as conn:
            conn.execute(stmt)
        bar.progress(i/len(files))
    st.success("インポート完了！")

# ---------- 可視化 ----------
with eng.connect() as conn:
    stores = [r[0] for r in conn.execute(sa.text(
        "SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'"))]
if stores:
    store_sel = st.selectbox("店舗を選択", stores)
    tbl = sa.Table(store_sel, sa.MetaData(), autoload_with=eng)
    df_show = pd.read_sql(sa.select(tbl).limit(1000), eng)
    st.dataframe(df_show)
