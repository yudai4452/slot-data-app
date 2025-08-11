import io
import re
import time
import random
import datetime as dt
import pandas as pd
import streamlit as st
import sqlalchemy as sa
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import altair as alt
import json

# ======================== 基本設定 ========================
st.set_page_config(page_title="Slot Manager", layout="wide")
st.title("🎰 Slot Data Manager & Visualizer")
mode = st.sidebar.radio("モード", ("📥 データ取り込み", "📊 可視化"))

SA_INFO = st.secrets["gcp_service_account"]
PG_CFG  = st.secrets["connections"]["slot_db"]

# キャッシュクリア
with st.sidebar:
    if st.button("♻️ キャッシュをクリア"):
        st.cache_data.clear()
        st.success("キャッシュをクリアしました。")
        st.rerun()

# ======================== 設定ファイル ========================
with open("setting.json", encoding="utf-8") as f:
    setting_map = json.load(f)

# ======================== 接続 ========================
@st.cache_resource
def gdrive():
    try:
        creds = Credentials.from_service_account_info(
            SA_INFO, scopes=["https://www.googleapis.com/auth/drive.readonly"]
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"Drive認証エラー: {e}")
        return None

drive = gdrive()

@st.cache_resource
def engine():
    try:
        url = (
            f"postgresql+psycopg2://{PG_CFG.username}:{PG_CFG.password}"
            f"@{PG_CFG.host}:{PG_CFG.port}/{PG_CFG.database}?sslmode=require"
        )
        return sa.create_engine(url, pool_pre_ping=True)
    except Exception as e:
        st.error(f"DB接続エラー: {e}")
        return None

eng = engine()
if eng is None:
    st.stop()

# ======================== カラム定義マッピング ========================
# 「最大持ち玉」と「最大持玉」の表記ゆれ両対応
COLUMN_MAP = {
    "メッセ武蔵境": {
        "台番号":           "台番号",
        "スタート回数":     "スタート回数",
        "累計スタート":     "累計スタート",
        "BB回数":          "BB回数",
        "RB回数":          "RB回数",
        "ART回数":         "ART回数",
        "最大持ち玉":       "最大持玉",
        "最大持玉":         "最大持玉",
        "BB確率":          "BB確率",
        "RB確率":          "RB確率",
        "ART確率":         "ART確率",
        "合成確率":        "合成確率",
        "前日最終スタート": "前日最終スタート",
    },
    "ジャンジャンマールゴット分倍河原": {
        "台番号":           "台番号",
        "累計スタート":     "累計スタート",
        "BB回数":          "BB回数",
        "RB回数":          "RB回数",
        "最大持ち玉":       "最大持玉",
        "最大持玉":         "最大持玉",
        "BB確率":          "BB確率",
        "RB確率":          "RB確率",
        "合成確率":        "合成確率",
        "前日最終スタート": "前日最終スタート",
        "スタート回数":     "スタート回数",
    },
    "プレゴ立川": {
        "台番号":           "台番号",
        "累計スタート":     "累計スタート",
        "BB回数":          "BB回数",
        "RB回数":          "RB回数",
        "最大差玉":         "最大差玉",
        "BB確率":          "BB確率",
        "RB確率":          "RB確率",
        "合成確率":        "合成確率",
        "前日最終スタート": "前日最終スタート",
        "スタート回数":     "スタート回数",
    },
}

# ======================== Google API リトライユーティリティ ========================
def gapi_call(req_builder, *a, **kw):
    """Google API 呼び出しに指数バックオフを適用"""
    for i in range(5):
        try:
            return req_builder(*a, **kw).execute()
        except Exception as e:
            if i == 4:
                raise
            time.sleep((2 ** i) + random.random())

# ======================== Drive: 再帰 + ページング ========================
@st.cache_data(ttl=300)
def list_csv_recursive(folder_id: str):
    if drive is None:
        raise RuntimeError("Drive未接続です")
    all_files, queue = [], [(folder_id, "")]
    while queue:
        fid, cur = queue.pop()
        page_token = None
        while True:
            res = gapi_call(
                drive.files().list,
                q=f"'{fid}' in parents and trashed=false",
                fields="nextPageToken, files(id,name,mimeType)",
                pageSize=1000, pageToken=page_token
            )
            for f in res.get("files", []):
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    queue.append((f["id"], f"{cur}/{f['name']}"))
                elif f["name"].lower().endswith(".csv"):
                    all_files.append({**f, "path": f"{cur}/{f['name']}"})
            page_token = res.get("nextPageToken")
            if not page_token:
                break
    return all_files

# ======================== メタ情報解析（正規表現で日付抽出） ========================
DATE_RE = re.compile(r"\d{4}-\d{2}-\d{2}")

def parse_meta(path: str):
    parts = path.strip("/").split("/")
    if len(parts) < 3:
        raise ValueError(f"パスが短すぎます: {path}")
    store, machine = parts[-3], parts[-2]
    m = DATE_RE.search(parts[-1])
    if not m:
        raise ValueError(f"ファイル名に日付(YYYY-MM-DD)が見つかりません: {parts[-1]}")
    date = dt.date.fromisoformat(m.group(0))
    return store, machine, date

# ======================== 正規化 ========================
def normalize(df_raw: pd.DataFrame, store: str) -> pd.DataFrame:
    df = df_raw.rename(columns=COLUMN_MAP[store])

    prob_cols = ["BB確率", "RB確率", "ART確率", "合成確率"]
    for col in prob_cols:
        if col not in df.columns:
            continue
        ser = df[col].astype(str)
        mask_div = ser.str.contains("/", na=False)

        # "1/x" 形式
        if mask_div.any():
            denom = pd.to_numeric(
                ser[mask_div].str.split("/", expand=True)[1],
                errors="coerce"
            )
            val = 1.0 / denom
            val[(denom <= 0) | (~denom.notna())] = 0  # 0/負/欠損は0に
            df.loc[mask_div, col] = val

        # 数値直書き（>1 は 1/値, <=1 はそのまま）
        num = pd.to_numeric(ser[~mask_div], errors="coerce")
        conv = num.copy()
        conv[num > 1] = 1.0 / num[num > 1]
        df.loc[~mask_div, col] = conv.fillna(0)

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)
        # 値域クランプ（わずかな外れも0〜1に収める）
        df[col] = df[col].clip(lower=0, upper=1)

    # 整数カラム
    int_cols = [
        "台番号", "累計スタート", "スタート回数", "BB回数",
        "RB回数", "ART回数", "最大持玉", "最大差玉", "前日最終スタート"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df

# ======================== 読み込み + 正規化 ========================
@st.cache_data(ttl=300)
def load_and_normalize(raw_bytes: bytes, store: str) -> pd.DataFrame:
    header = pd.read_csv(io.BytesIO(raw_bytes), encoding="shift_jis", nrows=0).columns.tolist()
    mapping_keys = list(dict.fromkeys(COLUMN_MAP[store].keys()))
    usecols = [col for col in mapping_keys if col in header]
    df_raw = pd.read_csv(
        io.BytesIO(raw_bytes),
        encoding="shift_jis",
        usecols=usecols,
        on_bad_lines="skip",
        engine="c",
    )
    return normalize(df_raw, store)

# ======================== テーブル作成（台番号の追加 & 型修正） ========================
def ensure_store_table(store: str):
    safe = "slot_" + store.replace(" ", "_")
    insp = inspect(eng)
    meta = sa.MetaData()
    if not insp.has_table(safe):
        cols = [
            sa.Column("date", sa.Date, nullable=False),
            sa.Column("機種", sa.Text, nullable=False),
            sa.Column("台番号", sa.Integer, nullable=False),
        ]
        unique_cols = list(dict.fromkeys(COLUMN_MAP[store].values()))
        numeric_int = {
            "台番号", "累計スタート", "スタート回数", "BB回数", "RB回数",
            "ART回数", "最大持玉", "最大差玉", "前日最終スタート"
        }
        for col_name in unique_cols:
            if col_name in {"date", "機種", "台番号"}:
                continue
            if col_name in numeric_int:
                cols.append(sa.Column(col_name, sa.Integer))
            else:
                cols.append(sa.Column(col_name, sa.Float))
        t = sa.Table(safe, meta, *cols, sa.PrimaryKeyConstraint("date", "機種", "台番号"))
        meta.create_all(eng)
        return t
    return sa.Table(safe, meta, autoload_with=eng)

def ensure_indexes(table_name: str, conn):
    conn.execute(sa.text(
        f"CREATE INDEX IF NOT EXISTS ix_{table_name}_date_machine ON {table_name}(date, 機種)"
    ))
    conn.execute(sa.text(
        f"CREATE INDEX IF NOT EXISTS ix_{table_name}_machine_slot_date ON {table_name}(機種, 台番号, date)"
    ))

# ======================== アップサート（新規/更新件数を返す） ========================
def upsert_dataframe(conn, table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
    rows = df.to_dict(orient="records")
    if not rows:
        return 0, 0
    stmt = pg_insert(table).values(rows)
    update_cols = {c.name: stmt.excluded[c.name] for c in table.c if c.name not in pk}
    # Postgresのxmaxで新規(0)/更新(!=0) を判定
    stmt = stmt.on_conflict_do_update(
        index_elements=list(pk),
        set_=update_cols
    ).returning(sa.literal_column("xmax"))
    res = conn.execute(stmt).fetchall()
    created = sum(1 for r in res if getattr(r, "xmax", 0) == 0)
    updated = len(res) - created
    return created, updated

# ========================= データ取り込み =========================
if mode == "📥 データ取り込み":
    st.header("Google Drive → Postgres インポート")
    folder_options = {
        "🧪 テスト用": "1MRQFPBahlSwdwhrqqBzudXL18y8-qOb8",
        "🚀 本番用":   "1hX8GQRuDm_E1A1Cu_fXorvwxv-XF7Ynl",
    }
    sel_label = st.selectbox("フォルダタイプ", list(folder_options.keys()))
    folder_id = st.text_input("Google Drive フォルダ ID", value=folder_options[sel_label])

    c1, c2 = st.columns(2)
    imp_start = c1.date_input("開始日", dt.date(2024, 1, 1), key="import_start_date")
    imp_end   = c2.date_input("終了日", dt.date.today(), key="import_end_date")

    if st.button("🚀 インポート実行", disabled=not folder_id):
        try:
            files = [
                f for f in list_csv_recursive(folder_id)
                if imp_start <= parse_meta(f["path"])[2] <= imp_end
            ]
        except Exception as e:
            st.error(f"ファイル一覧取得エラー: {e}")
            st.stop()

        st.write(f"🔍 対象 CSV: **{len(files)} 件**")
        bar = st.progress(0.0)
        current_file = st.empty()
        created_tables = {}
        total_new = total_upd = 0
        errors = []

        for i, f in enumerate(files, 1):
            current_file.text(f"処理中ファイル: {f['path']}")
            try:
                raw = gapi_call(drive.files().get_media, fileId=f["id"])
                store, machine, date = parse_meta(f["path"])
                table_name = "slot_" + store.replace(" ", "_")

                if table_name not in created_tables:
                    tbl = ensure_store_table(store)
                    created_tables[table_name] = tbl
                else:
                    tbl = created_tables[table_name]

                df = load_and_normalize(raw, store)
                if df.empty:
                    bar.progress(i / len(files))
                    continue

                df["機種"], df["date"] = machine, date
                # テーブルに存在する列のみ
                valid_cols = [c.name for c in tbl.c]
                df = df[[c for c in df.columns if c in valid_cols]]

                with eng.begin() as conn:
                    n_new, n_upd = upsert_dataframe(conn, tbl, df)
                    ensure_indexes(tbl.name, conn)
                    total_new += n_new
                    total_upd += n_upd

            except Exception as e:
                errors.append({"file": f["path"], "error": str(e)})

            bar.progress(i / len(files))

        current_file.text("")

        if errors:
            st.error(f"⚠️ {len(errors)}件のファイルで失敗しました。下の表を確認してください。")
            st.dataframe(pd.DataFrame(errors))

        st.success(f"インポート完了！ 新規: {total_new:,} / 更新: {total_upd:,}")

# ========================= 可視化モード =========================
if mode == "📊 可視化":
    st.header("DB 可視化")

    # テーブル一覧
    try:
        with eng.connect() as conn:
            tables = [r[0] for r in conn.execute(sa.text(
                "SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'"
            ))]
    except Exception as e:
        st.error(f"テーブル一覧取得エラー: {e}")
        st.stop()

    if not tables:
        st.info("まず取り込みモードでデータを入れてください。")
        st.stop()

    table_name = st.selectbox("テーブル選択", tables)
    if not table_name:
        st.error("テーブルが選択されていません")
        st.stop()

    tbl = sa.Table(table_name, sa.MetaData(), autoload_with=eng)

    # 最小/最大日付
    with eng.connect() as conn:
        row = conn.execute(sa.text(f"SELECT MIN(date), MAX(date) FROM {table_name}")).first()
        min_date, max_date = (row or (None, None))

    if not (min_date and max_date):
        st.info("このテーブルには日付データがありません。まず取り込みを実行してください。")
        st.stop()

    # 直近90日を初期値（範囲外なら min/max に丸め）
    default_start = max(min_date, max_date - dt.timedelta(days=89))
    default_end = max_date

    c1, c2 = st.columns(2)
    vis_start = c1.date_input(
        "開始日", value=default_start, min_value=min
