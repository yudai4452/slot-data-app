import io, datetime as dt, pandas as pd, streamlit as st
import sqlalchemy as sa
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from sqlalchemy.dialects.postgresql import insert as pg_insert

def list_csv_recursive(folder_id: str):
    """folder_id 以下を再帰的にめぐり .csv ファイルを返す"""
    all_files, queue = [], [folder_id]
    while queue:
        fid = queue.pop()
        res = drive.files().list(
            q=f"'{fid}' in parents and trashed=false",
            fields="files(id,name,mimeType)",
            pageSize=1000,
            supportsAllDrives=True,
        ).execute()
        for f in res.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                queue.append(f["id"])              # ← サブフォルダをキューへ
            elif f["name"].lower().endswith(".csv"):
                all_files.append(f)                # ← CSV を収集
    return all_files

# ---- Streamlit 画面設定 ----
st.set_page_config(page_title="Slot Manager", layout="wide")
st.title("🎰 Slot Data Manager")

# ---- Secrets 読み込み ----
SA_INFO = st.secrets["gcp_service_account"]
PG_CFG  = st.secrets["connections"]["slot_db"]

# ---- Google Drive 接続 ----
@st.cache_resource
def gdrive():
    creds = Credentials.from_service_account_info(
        SA_INFO, scopes=["https://www.googleapis.com/auth/drive.readonly"]
    )
    return build("drive", "v3", credentials=creds)
drive = gdrive()

# ---- Postgres 接続 ----
@st.cache_resource
def engine():
    url = (
        f"postgresql+psycopg2://{PG_CFG.username}:{PG_CFG.password}"
        f"@{PG_CFG.host}:{PG_CFG.port}/{PG_CFG.database}"
        "?sslmode=require"
    )
    return sa.create_engine(url, pool_pre_ping=True)
eng = engine()

# ---- 1 回だけテーブル作成 ----
with eng.begin() as conn:
    conn.exec_driver_sql("""
    CREATE TABLE IF NOT EXISTS slot_data (
      store TEXT, machine TEXT, date DATE, "台番号" INT,
      "累計スタート" INT, "スタート回数" INT,
      "BB回数" INT, "RB回数" INT, "最大差玉" INT,
      PRIMARY KEY (store, machine, date, "台番号")
    )""")

# ---- Drive から CSV 取得 → DB へ ----
folder_id = st.text_input("Google Drive フォルダ ID")
if st.button("🚀 インポート"):
    files = list_csv_recursive(folder_id)
    bar = st.progress(0.0)
    for i, f in enumerate(files, 1):
        raw = drive.files().get_media(fileId=f["id"]).execute()
        df  = pd.read_csv(io.BytesIO(raw), encoding="shift_jis")
        # 例: 店舗・機種・日付をここで手動入力（本格化は後工程）
        store  = st.text_input("店舗", key=f's{i}')
        machine= st.text_input("機種", key=f'm{i}')
        date   = dt.date.today()
        df["store"]=store; df["machine"]=machine; df["date"]=date
        with eng.begin() as conn:
            stmt = pg_insert(sa.Table("slot_data", sa.MetaData(), autoload_with=conn)).\
                   values(df.to_dict("records")).on_conflict_do_nothing()
            conn.execute(stmt)
        bar.progress(i/len(files))
    st.write(f"🔍 見つかった CSV: {len(files)} 件")

# ---- 可視化 ----
with eng.connect() as conn:
    df = pd.read_sql("SELECT * FROM slot_data LIMIT 100", conn)
st.dataframe(df)
