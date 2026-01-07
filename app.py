# app.py
import io
import re
import json
import datetime as dt
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from uuid import uuid4

import altair as alt
import numpy as np
import pandas as pd
import sqlalchemy as sa
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from sqlalchemy import inspect
from sqlalchemy.dialects.postgresql import insert as pg_insert

# ============================================================
# Streamlit 基本設定
# ============================================================
st.set_page_config(page_title="Slot Manager", layout="wide")

MODE_IMPORT = "📥 データ取り込み"
MODE_VIZ = "📊 可視化"
MODE_ML = "🧠 MLデータ作成（予測UI付き）"

mode = st.sidebar.radio("モード", (MODE_IMPORT, MODE_VIZ, MODE_ML), key="mode_radio")
st.title("🎰 Slot Data Manager & Visualizer")

# ============================================================
# シークレット / 設定ファイル読み込み
# ============================================================
SA_INFO = st.secrets["gcp_service_account"]
PG_CFG = st.secrets["connections"]["slot_db"]

with open("setting.json", encoding="utf-8") as f:
    setting_map = json.load(f)

# ============================================================
# DB & Google Drive 接続
# ============================================================
def make_drive():
    """都度 Credentials から Drive クライアントを生成（スレッドセーフのために毎回作る用）"""
    try:
        creds = Credentials.from_service_account_info(
            SA_INFO,
            scopes=["https://www.googleapis.com/auth/drive.readonly"],
        )
        return build("drive", "v3", credentials=creds)
    except Exception as e:
        st.error(f"Drive認証エラー: {e}")
        return None


@st.cache_resource
def gdrive():
    """シンプルな Drive クライアント（スレッドを使わない処理向け）"""
    return make_drive()


drive = gdrive()


@st.cache_resource
def engine():
    """Postgres エンジン作成（接続は必要時に毎回 open/close）"""
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

# ============================================================
# 共通: Postgres 識別子クオート / ファイル名安全化
# ============================================================
def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def safe_index_name(table_name: str, suffix: str) -> str:
    base = re.sub(r"[^0-9a-zA-Z_]+", "_", table_name)
    base = re.sub(r"_+", "_", base).strip("_") or "slot"
    return f"{base}_{suffix}"


def safe_filename(s: str) -> str:
    return re.sub(r'[\\/:*?"<>|]+', "_", s)


# ============================================================
# カラム正規化用マッピング
# ============================================================
COLUMN_MAP = {
    "メッセ武蔵境": {
        "台番号": "台番号",
        "スタート回数": "スタート回数",
        "累計スタート": "累計スタート",
        "BB回数": "BB回数",
        "RB回数": "RB回数",
        "ART回数": "ART回数",
        "最大持ち玉": "最大持玉",
        "最大持玉": "最大持玉",
        "BB確率": "BB確率",
        "RB確率": "RB確率",
        "ART確率": "ART確率",
        "合成確率": "合成確率",
        "前日最終スタート": "前日最終スタート",
    },
    "ジャンジャンマールゴット分倍河原": {
        "台番号": "台番号",
        "累計スタート": "累計スタート",
        "BB回数": "BB回数",
        "RB回数": "RB回数",
        "最大持ち玉": "最大持玉",
        "最大持玉": "最大持玉",
        "BB確率": "BB確率",
        "RB確率": "RB確率",
        "合成確率": "合成確率",
        "前日最終スタート": "前日最終スタート",
        "スタート回数": "スタート回数",
    },
    "プレゴ立川": {
        "台番号": "台番号",
        "累計スタート": "累計スタート",
        "BB回数": "BB回数",
        "RB回数": "RB回数",
        "最大差玉": "最大差玉",
        "BB確率": "BB確率",
        "RB確率": "RB確率",
        "合成確率": "合成確率",
        "前日最終スタート": "前日最終スタート",
        "スタート回数": "スタート回数",
    },
}

# 1/x 表記したい「確率系」カラム
PROB_PLOT_COLUMNS = ["合成確率", "BB確率", "RB確率", "ART確率"]

# デフォルトで選択したい「出玉系」カラム（上から順に優先）
DEFAULT_PAYOUT_COLUMNS = ["最大差玉", "差枚", "差玉", "最大持玉"]

# ============================================================
# ML用: 差枚相当ターゲット（店ごとの違い吸収）
# ============================================================
PAYOUT_TARGET_PRIORITY = ["差枚", "差玉", "最大差玉", "最大持玉"]
PAYOUT_ALIASES = {
    "差枚": ["差枚", "差枚数", "差枚(枚)"],
    "差玉": ["差玉", "差玉数"],
    "最大差玉": ["最大差玉", "最大差枚", "最大差枚数"],
    "最大持玉": ["最大持玉", "最大持ち玉"],
}


def build_payout_candidates(numeric_candidates: list[str]) -> list[dict]:
    out = []
    seen_source = set()
    for canon in PAYOUT_TARGET_PRIORITY:
        for src in PAYOUT_ALIASES.get(canon, [canon]):
            if src in numeric_candidates and src not in seen_source:
                seen_source.add(src)
                out.append(
                    {
                        "canonical": canon,
                        "source": src,
                        "label": f"{canon}相当：{src}",
                    }
                )
    return out


# ============================================================
# Google Drive: フォルダ以下の CSV を再帰的に取得
# ============================================================
@st.cache_data
def list_csv_recursive(folder_id: str):
    if drive is None:
        raise RuntimeError("Drive未接続です")

    all_files = []
    queue = [(folder_id, "")]  # (folder_id, path_prefix)

    while queue:
        fid, cur = queue.pop()
        page_token = None

        while True:
            res = (
                drive.files()
                .list(
                    q=f"'{fid}' in parents and trashed=false",
                    fields="nextPageToken, files(id,name,mimeType,md5Checksum,modifiedTime,size)",
                    pageSize=1000,
                    pageToken=page_token,
                )
                .execute()
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


# ============================================================
# パスから 店舗 / 機種 / 日付 を抽出
# ============================================================
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


# ============================================================
# CSV → DataFrame 正規化
# ============================================================
def normalize(df_raw: pd.DataFrame, store: str) -> pd.DataFrame:
    df = df_raw.rename(columns=COLUMN_MAP[store])

    # 確率系を 0〜1 に統一
    prob_cols = ["BB確率", "RB確率", "ART確率", "合成確率"]
    for col in prob_cols:
        if col not in df.columns:
            continue

        ser = df[col].astype(str)
        mask_div = ser.str.contains("/", na=False)

        # "1/113"
        if mask_div.any():
            denom = pd.to_numeric(ser[mask_div].str.split("/", expand=True)[1], errors="coerce")
            val = 1.0 / denom
            val[(denom <= 0) | (~denom.notna())] = 0
            df.loc[mask_div, col] = val

        # "113" → 1/113
        num = pd.to_numeric(ser[~mask_div], errors="coerce")
        conv = num.copy()
        conv[num > 1] = 1.0 / num[num > 1]
        conv = conv.fillna(0)
        df.loc[~mask_div, col] = conv

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    # 整数系
    int_cols = [
        "台番号",
        "累計スタート",
        "スタート回数",
        "BB回数",
        "RB回数",
        "ART回数",
        "最大持玉",
        "最大差玉",
        "前日最終スタート",
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df


def load_and_normalize(raw_bytes: bytes, store: str) -> pd.DataFrame:
    header = pd.read_csv(io.BytesIO(raw_bytes), encoding="shift_jis", nrows=0).columns.tolist()
    mapping_keys = list(dict.fromkeys(COLUMN_MAP[store].keys()))
    usecols = [col for col in mapping_keys if col in header]

    df_raw = pd.read_csv(
        io.BytesIO(raw_bytes),
        encoding="shift_jis",
        usecols=usecols,
        on_bad_lines="skip",
        engine="python",
    )
    return normalize(df_raw, store)


# ============================================================
# import_log テーブル（差分取り込み管理）
# ============================================================
def ensure_import_log_table():
    meta = sa.MetaData()
    insp = inspect(eng)

    if not insp.has_table("import_log"):
        t = sa.Table(
            "import_log",
            meta,
            sa.Column("file_id", sa.Text, primary_key=True),
            sa.Column("md5", sa.Text, nullable=False),
            sa.Column("path", sa.Text, nullable=False),
            sa.Column("store", sa.Text, nullable=False),
            sa.Column("machine", sa.Text, nullable=False),
            sa.Column("date", sa.Date, nullable=False),
            sa.Column("rows", sa.Integer, nullable=False),
            sa.Column("imported_at", sa.DateTime, nullable=False, server_default=sa.func.now()),
        )
        meta.create_all(eng)
    else:
        t = sa.Table("import_log", meta, autoload_with=eng)

    return t


def get_imported_md5_map():
    log = ensure_import_log_table()
    with eng.connect() as conn:
        rows = conn.execute(sa.select(log.c.file_id, log.c.md5)).fetchall()
    return {r[0]: r[1] for r in rows}


def upsert_import_log(entries: list[dict]):
    if not entries:
        return
    log = ensure_import_log_table()
    stmt = pg_insert(log).values(entries)
    stmt = stmt.on_conflict_do_update(
        index_elements=[log.c.file_id],
        set_={
            "md5": stmt.excluded.md5,
            "path": stmt.excluded.path,
            "store": stmt.excluded.store,
            "machine": stmt.excluded.machine,
            "date": stmt.excluded.date,
            "rows": stmt.excluded.rows,
            "imported_at": sa.func.now(),
        },
    )
    with eng.begin() as conn:
        conn.execute(stmt)


# ============================================================
# 店舗ごとの slot_* テーブルを作成
# ============================================================
def ensure_store_table(store: str) -> sa.Table:
    safe_name = "slot_" + store.replace(" ", "_")
    insp = inspect(eng)
    meta = sa.MetaData()

    if not insp.has_table(safe_name):
        cols = [
            sa.Column("date", sa.Date, nullable=False),
            sa.Column("機種", sa.Text, nullable=False),
            sa.Column("台番号", sa.Integer, nullable=False),
        ]

        unique_cols = list(dict.fromkeys(COLUMN_MAP[store].values()))
        numeric_int = {
            "台番号",
            "累計スタート",
            "スタート回数",
            "BB回数",
            "RB回数",
            "ART回数",
            "最大持玉",
            "最大差玉",
            "前日最終スタート",
        }

        for col_name in unique_cols:
            if col_name in {"date", "機種", "台番号"}:
                continue
            if col_name in numeric_int:
                cols.append(sa.Column(col_name, sa.Integer))
            else:
                cols.append(sa.Column(col_name, sa.Float))

        t = sa.Table(
            safe_name,
            meta,
            *cols,
            sa.PrimaryKeyConstraint("date", "機種", "台番号"),
        )
        meta.create_all(eng)
        return t

    return sa.Table(safe_name, meta, autoload_with=eng)


# ============================================================
# 通常 UPSERT（行ごと）
# ============================================================
def upsert_dataframe(conn, table: sa.Table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
    rows = df.to_dict(orient="records")
    if not rows:
        return
    stmt = pg_insert(table).values(rows)
    update_cols = {c.name: stmt.excluded[c.name] for c in table.c if c.name not in pk}
    stmt = stmt.on_conflict_do_update(index_elements=list(pk), set_=update_cols)
    conn.execute(stmt)


# ============================================================
# COPY → MERGE で高速アップサート
# ============================================================
def bulk_upsert_copy_merge(table: sa.Table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
    if df.empty:
        return

    valid_cols = [c.name for c in table.c]
    cols = [c for c in df.columns if c in valid_cols]

    for p in pk:
        if p not in cols:
            raise ValueError(f"COPY列に主キー {p} が含まれていません")

    df_use = df[cols].copy()

    csv_buf = io.StringIO()
    df_use.to_csv(csv_buf, index=False, na_rep="")
    csv_text = csv_buf.getvalue()

    tmp_name = f"tmp_{table.name}_{uuid4().hex[:8]}"
    cols_q = ", ".join(q(c) for c in cols)
    pk_q = ", ".join(q(p) for p in pk)
    upd_cols = [c for c in cols if c not in pk]
    set_clause = ", ".join(f"{q(c)}=EXCLUDED.{q(c)}" for c in upd_cols) if upd_cols else ""

    create_tmp_sql = f"CREATE TEMP TABLE {q(tmp_name)} (LIKE {q(table.name)} INCLUDING ALL);"
    copy_sql = f"COPY {q(tmp_name)} ({cols_q}) FROM STDIN WITH (FORMAT csv, HEADER true);"
    insert_sql = (
        f"INSERT INTO {q(table.name)} ({cols_q}) "
        f"SELECT {cols_q} FROM {q(tmp_name)} "
        f"ON CONFLICT ({pk_q}) DO "
        + ("NOTHING;" if not set_clause else f"UPDATE SET {set_clause};")
    )
    drop_tmp_sql = f"DROP TABLE IF EXISTS {q(tmp_name)};"

    with eng.begin() as conn:
        driver_conn = getattr(conn.connection, "driver_connection", None)
        if driver_conn is None:
            driver_conn = conn.connection.connection  # psycopg2 fallback
        with driver_conn.cursor() as cur:
            cur.execute(create_tmp_sql)
            cur.copy_expert(copy_sql, io.StringIO(csv_text))
            cur.execute(insert_sql)
            cur.execute(drop_tmp_sql)


# ============================================================
# 並列処理: CSV ダウンロード & 正規化
# ============================================================
def process_one_file(file_meta: dict) -> dict | None:
    try:
        store, machine, date = parse_meta(file_meta["path"])
        if store not in COLUMN_MAP:
            return None

        drv = make_drive()
        raw = drv.files().get_media(fileId=file_meta["id"]).execute()
        df = load_and_normalize(raw, store)
        if df.empty:
            return None

        df["機種"] = machine
        df["date"] = date
        table_name = "slot_" + store.replace(" ", "_")

        return {
            "table_name": table_name,
            "df": df,
            "store": store,
            "machine": machine,
            "date": date,
            "file_id": file_meta["id"],
            "md5": file_meta.get("md5Checksum") or "",
            "path": file_meta["path"],
        }
    except Exception as e:
        return {"error": f"{file_meta.get('path', '(unknown)')} 処理エラー: {e}"}


def run_import_for_targets(targets: list[dict], workers: int, use_copy: bool):
    status = st.empty()
    created_tables: dict[str, sa.Table] = {}
    import_log_entries = []
    errors = []
    bucket: dict[str, list[dict]] = defaultdict(list)

    # 1) 並列でCSV取得
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(process_one_file, f): f for f in targets}
        for fut in as_completed(futures):
            res = fut.result()
            if res is None:
                continue
            if "error" in res:
                errors.append(res["error"])
                continue
            bucket[res["table_name"]].append(res)
            status.text(f"処理完了: {res['path']}")

    # 2) テーブルごとにDB書き込み
    for table_name, items in bucket.items():
        if table_name not in created_tables:
            tbl = ensure_store_table(items[0]["store"])
            created_tables[table_name] = tbl
        else:
            tbl = created_tables[table_name]

        valid_cols = [c.name for c in tbl.c]

        if use_copy:
            try:
                dfs = []
                for res in items:
                    df = res["df"]
                    for c in valid_cols:
                        if c not in df.columns:
                            df[c] = pd.NA
                    dfs.append(df[[c for c in df.columns if c in valid_cols]])

                df_all = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=valid_cols)
                bulk_upsert_copy_merge(tbl, df_all)

            except Exception as e:
                errors.append(f"{table_name} COPY高速化失敗のため通常UPSERTで再試行: {e}")
                with eng.begin() as conn:
                    for res in items:
                        df_one = res["df"][[c for c in res["df"].columns if c in valid_cols]]
                        try:
                            upsert_dataframe(conn, tbl, df_one)
                        except Exception as ie:
                            errors.append(f"{res['path']} 通常UPSERTでも失敗: {ie}")
        else:
            with eng.begin() as conn:
                for res in items:
                    df_one = res["df"][[c for c in res["df"].columns if c in valid_cols]]
                    upsert_dataframe(conn, tbl, df_one)

        for res in items:
            import_log_entries.append(
                {
                    "file_id": res["file_id"],
                    "md5": res["md5"],
                    "path": res["path"],
                    "store": res["store"],
                    "machine": res["machine"],
                    "date": res["date"],
                    "rows": int(len(res["df"])),
                }
            )

    processed_files = sum(len(v) for v in bucket.values())
    return import_log_entries, errors, processed_files


# ============================================================
# 時系列基盤モデル（UI実行用）
# ============================================================
@st.cache_resource(show_spinner=False)
def get_chronos2_pipeline(device_map: str = "cpu"):
    from chronos import Chronos2Pipeline
    return Chronos2Pipeline.from_pretrained("amazon/chronos-2", device_map=device_map)


@st.cache_resource(show_spinner=False)
def get_timesfm_model():
    import torch
    import timesfm

    torch.set_float32_matmul_precision("high")
    model = timesfm.TimesFM_2p5_200M_torch.from_pretrained("google/timesfm-2.5-200m-pytorch")
    try:
        model.compile(
            timesfm.ForecastConfig(
                max_context=1024,
                max_horizon=256,
                normalize_inputs=True,
            )
        )
    except Exception:
        pass
    return model


def forecast_with_chronos2(df_long: pd.DataFrame, horizon: int, device_map: str = "cpu") -> pd.DataFrame:
    pipe = get_chronos2_pipeline(device_map=device_map)

    pred = pipe.predict_df(
        df_long,
        prediction_length=horizon,
        quantile_levels=[0.1, 0.5, 0.9],
        id_column="id",
        timestamp_column="timestamp",
        target="target",
    )

    if "0.5" in pred.columns:
        pred = pred.rename(columns={"0.5": "yhat"})
    elif "predictions" in pred.columns:
        pred = pred.rename(columns={"predictions": "yhat"})
    else:
        num_cols = [
            c
            for c in pred.columns
            if c not in {"id", "timestamp"} and pd.api.types.is_numeric_dtype(pred[c])
        ]
        if not num_cols:
            raise RuntimeError(f"Chronos-2の出力列が想定と違います: {pred.columns.tolist()}")
        pred = pred.rename(columns={num_cols[0]: "yhat"})

    keep = [c for c in ["id", "timestamp", "yhat", "0.1", "0.9"] if c in pred.columns]
    return pred[keep].copy()


def forecast_with_timesfm(df_long: pd.DataFrame, horizon: int, freq: str = "D") -> pd.DataFrame:
    model = get_timesfm_model()

    df_long = df_long.sort_values(["id", "timestamp"]).copy()
    ids = df_long["id"].unique().tolist()

    series_list = []
    last_ts = {}

    for _id in ids:
        g = df_long[df_long["id"] == _id].sort_values("timestamp")
        y = g["target"].astype(float)
        y = y.interpolate(limit_direction="both").fillna(0.0)
        series_list.append(y.to_numpy())
        last_ts[_id] = pd.to_datetime(g["timestamp"].max())

    point_fcst, _ = model.forecast(horizon=horizon, inputs=series_list)

    rows = []
    for i, _id in enumerate(ids):
        start = last_ts[_id]
        future_index = pd.date_range(start=start, periods=horizon + 1, freq=freq)[1:]
        for t, ts in enumerate(future_index):
            rows.append({"id": _id, "timestamp": ts, "yhat": float(point_fcst[i, t])})

    return pd.DataFrame(rows)


def prob_to_denom(p: float) -> float:
    if p is None or (not np.isfinite(p)) or p <= 0:
        return float("inf")
    return 1.0 / float(p)


def score_setting_by_denom(pred_prob: float, thresholds: dict) -> str | None:
    if not thresholds:
        return None

    d = prob_to_denom(pred_prob)
    best = None
    best_dist = float("inf")

    for k, v in thresholds.items():
        try:
            vv = float(v)
        except Exception:
            continue
        dv = prob_to_denom(vv)
        dist = abs(d - dv)
        if dist < best_dist:
            best_dist = dist
            best = k

    return best


# ============================================================
# 📥 データ取り込みモード
# ============================================================
if mode == MODE_IMPORT:
    st.header("Google Drive → Postgres インポート")

    folder_options = {
        "🧪 テスト用": "1MRQFPBahlSwdwhrqqBzudXL18y8-qOb8",
        "🚀 本番用": "1hX8GQRuDm_E1A1Cu_fXorvwxv-XF7Ynl",
    }
    options = list(folder_options.keys())
    default_idx = options.index("🚀 本番用") if "🚀 本番用" in options else 0

    sel_label = st.selectbox("フォルダタイプ", options, index=default_idx, key="folder_type")
    folder_id = st.text_input("Google Drive フォルダ ID", value=folder_options[sel_label], key="folder_id")

    c1, c2 = st.columns(2)
    imp_start = c1.date_input("開始日", dt.date(2024, 1, 1), key="import_start_date")
    imp_end = c2.date_input("終了日", dt.date.today(), key="import_end_date")

    c3, c4 = st.columns(2)
    max_files = c3.slider(
        "最大ファイル数（1回の実行上限）",
        10,
        2000,
        300,
        step=10,
        help="大量フォルダは分割して取り込み（タイムアウト回避）",
        key="max_files",
    )
    workers = c4.slider(
        "並列ダウンロード数",
        1,
        8,
        4,
        help="並列数が多すぎるとAPI制限に当たる可能性があります",
        key="workers",
    )

    use_copy = st.checkbox(
        "DB書き込みをCOPYで高速化（推奨）",
        value=True,
        help="一時テーブルにCOPY→まとめてUPSERT。失敗時は自動で通常UPSERTにフォールバックします。",
        key="use_copy",
    )
    auto_batch = st.checkbox("最大ファイル数ごとに自動で続きのバッチも実行する", value=False, key="auto_batch")
    max_batches = st.number_input(
        "最大バッチ回数",
        min_value=1,
        max_value=100,
        value=3,
        help="実行時間が長くなりすぎるのを防ぐための上限",
        key="max_batches",
    )

    if st.button("🚀 インポート実行", disabled=not folder_id, key="import_run"):
        try:
            files_all = list_csv_recursive(folder_id)
            files = [f for f in files_all if imp_start <= parse_meta(f["path"])[2] <= imp_end]
        except Exception as e:
            st.error(f"ファイル一覧取得エラー: {e}")
            st.stop()

        imported_md5 = get_imported_md5_map()
        all_targets = [f for f in files if imported_md5.get(f["id"], "") != (f.get("md5Checksum") or "")]
        if not all_targets:
            st.success("差分はありません（すべて最新）")
            st.stop()

        all_targets.sort(key=lambda f: parse_meta(f["path"])[2])
        batches = [all_targets[i : i + max_files] for i in range(0, len(all_targets), max_files)]
        if not auto_batch:
            batches = batches[:1]

        total_files = sum(len(b) for b in batches[: int(max_batches)])
        done_files = 0
        bar = st.progress(0.0)
        status = st.empty()
        all_errors = []

        for bi, batch in enumerate(batches[: int(max_batches)], start=1):
            status.text(f"バッチ {bi}/{len(batches)}（{len(batch)} 件）を処理中…")
            entries, errors, processed_files = run_import_for_targets(batch, workers, use_copy)
            upsert_import_log(entries)
            all_errors.extend(errors)

            done_files += processed_files
            bar.progress(min(1.0, done_files / max(1, total_files)))

        status.text("")

        if len(batches) > max_batches and auto_batch:
            remaining = sum(len(b) for b in batches[int(max_batches) :])
            st.info(f"最大バッチ回数に達しました。残り {remaining} 件は、再度ボタンを押すと続きから処理します。")

        if all_errors:
            st.warning("一部でエラーが発生しました。詳細：")
            for msg in all_errors[:50]:
                st.write("- " + msg)
            if len(all_errors) > 50:
                st.write(f"... ほか {len(all_errors) - 50} 件")

        st.success(f"インポート完了（処理ファイル: {done_files} 件）！")


# ============================================================
# 📊 可視化モード
# ============================================================
if mode == MODE_VIZ:
    st.header("DB 可視化")

    try:
        with eng.connect() as conn:
            tables = [r[0] for r in conn.execute(sa.text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'"))]
    except Exception as e:
        st.error(f"テーブル一覧取得エラー: {e}")
        st.stop()

    if not tables:
        st.info("まず取り込みモードでデータを入れてください。")
        st.stop()

    default_table = "slot_プレゴ立川"
    default_index = next((i for i, t in enumerate(tables) if t == default_table), 0)

    table_name = st.selectbox("テーブル選択", tables, index=default_index, key="table_select")
    if not table_name:
        st.error("テーブルが選択されていません")
        st.stop()

    TBL_Q = q(table_name)

    @st.cache_data(ttl=600)
    def get_date_range(table_name: str):
        TBL_Q_inner = q(table_name)
        with eng.connect() as conn:
            row = conn.execute(sa.text(f"SELECT MIN(date), MAX(date) FROM {TBL_Q_inner}")).first()
        return (row[0], row[1]) if row else (None, None)

    min_date, max_date = get_date_range(table_name)
    if not (min_date and max_date):
        st.info("このテーブルには日付データがありません。まず取り込みを実行してください。")
        st.stop()

    c1, c2 = st.columns(2)
    vis_start = c1.date_input("開始日", value=min_date, min_value=min_date, max_value=max_date, key=f"visual_start_{table_name}")
    vis_end = c2.date_input("終了日", value=max_date, min_value=min_date, max_value=max_date, key=f"visual_end_{table_name}")

    idx_ok = st.checkbox("読み込み高速化のためのインデックスを作成（推奨・一度だけ）", value=True, key="create_index")
    if idx_ok:
        try:
            ix1 = safe_index_name(table_name, "ix_machine_date")
            ix2 = safe_index_name(table_name, "ix_machine_slot_date")
            with eng.begin() as conn:
                conn.execute(sa.text(f'CREATE INDEX IF NOT EXISTS {q(ix1)} ON {TBL_Q} ("機種","date");'))
                conn.execute(sa.text(f'CREATE INDEX IF NOT EXISTS {q(ix2)} ON {TBL_Q} ("機種","台番号","date");'))
        except Exception as e:
            st.info(f"インデックス作成をスキップ: {e}")

    @st.cache_data(ttl=600)
    def get_machines_fast(table_name: str, start: dt.date, end: dt.date):
        TBL_Q_inner = q(table_name)
        sql = sa.text(
            f'SELECT DISTINCT "機種" FROM {TBL_Q_inner} '
            f"WHERE date BETWEEN :s AND :e ORDER BY \"機種\""
        )
        with eng.connect() as conn:
            return [r[0] for r in conn.execute(sql, {"s": start, "e": end})]

    machines = get_machines_fast(table_name, vis_start, vis_end)
    if not machines:
        st.warning("指定期間にデータがありません")
        st.stop()

    machine_sel = st.selectbox("機種選択", machines, key="machine_select")
    show_avg = st.checkbox("全台平均を表示", value=False, key="show_avg")

    insp = inspect(eng)
    cols_info = insp.get_columns(table_name)

    numeric_candidates: list[str] = []
    for c in cols_info:
        name = c["name"]
        if name in {"date", "機種", "台番号"}:
            continue
        col_type = str(c["type"]).upper()
        if any(t in col_type for t in ("INT", "NUMERIC", "REAL", "DOUBLE", "FLOAT")):
            numeric_candidates.append(name)

    if not numeric_candidates:
        st.error("プロット可能な数値カラムが見つかりません。")
        st.stop()

    numeric_candidates = sorted(numeric_candidates, key=lambda n: (0 if n in PROB_PLOT_COLUMNS else 1, n))

    payout_candidates = [c for c in DEFAULT_PAYOUT_COLUMNS if c in numeric_candidates]
    if payout_candidates:
        default_metric = payout_candidates[0]
    elif "合成確率" in numeric_candidates:
        default_metric = "合成確率"
    else:
        default_metric = numeric_candidates[0]

    metric_col = st.selectbox("表示する項目", numeric_candidates, index=numeric_candidates.index(default_metric), key="metric_select")
    is_prob_metric = metric_col in PROB_PLOT_COLUMNS

    @st.cache_data(ttl=600)
    def get_slots_fast(table_name: str, machine: str, start: dt.date, end: dt.date):
        TBL_Q_inner = q(table_name)
        sql = sa.text(
            f"""
            SELECT DISTINCT "台番号"
            FROM {TBL_Q_inner}
            WHERE "機種" = :m
              AND date BETWEEN :s AND :e
              AND "台番号" IS NOT NULL
            ORDER BY "台番号"
            """
        )
        with eng.connect() as conn:
            vals = [r[0] for r in conn.execute(sql, {"m": machine, "s": start, "e": end})]
        return [int(v) for v in vals if v is not None]

    @st.cache_data(ttl=300)
    def fetch_plot_avg(table_name: str, machine: str, metric: str, start: dt.date, end: dt.date) -> pd.DataFrame:
        TBL_Q_inner = q(table_name)
        COL_Q = q(metric)
        sql = sa.text(
            f"""
            SELECT date, AVG({COL_Q}) AS plot_val
            FROM {TBL_Q_inner}
            WHERE "機種" = :m
              AND date BETWEEN :s AND :e
            GROUP BY date
            ORDER BY date
            """
        )
        with eng.connect() as conn:
            return pd.read_sql(sql, conn, params={"m": machine, "s": start, "e": end})

    @st.cache_data(ttl=300)
    def fetch_plot_slot(table_name: str, machine: str, metric: str, slot: int, start: dt.date, end: dt.date) -> pd.DataFrame:
        TBL_Q_inner = q(table_name)
        COL_Q = q(metric)
        sql = sa.text(
            f"""
            SELECT date, {COL_Q} AS plot_val
            FROM {TBL_Q_inner}
            WHERE "機種" = :m
              AND "台番号" = :n
              AND date BETWEEN :s AND :e
            ORDER BY date
            """
        )
        with eng.connect() as conn:
            return pd.read_sql(sql, conn, params={"m": machine, "n": int(slot), "s": start, "e": end})

    if show_avg:
        df_plot = fetch_plot_avg(table_name, machine_sel, metric_col, vis_start, vis_end)
        title = f"📈 全台平均 {metric_col} | {machine_sel}"
    else:
        slots = get_slots_fast(table_name, machine_sel, vis_start, vis_end)
        if not slots:
            st.warning("台番号のデータが見つかりません")
            st.stop()
        slot_sel = st.selectbox("台番号", slots, key="slot_select")
        df_plot = fetch_plot_slot(table_name, machine_sel, metric_col, slot_sel, vis_start, vis_end)
        title = f"📈 {metric_col} | {machine_sel} | 台 {slot_sel}"

    if df_plot is None or df_plot.empty:
        st.info("この条件では表示データがありません。期間や機種を変更してください。")
        st.stop()

    df_plot = df_plot.copy()
    df_plot["date"] = pd.to_datetime(df_plot["date"])
    xdomain_start = df_plot["date"].min()
    xdomain_end = df_plot["date"].max()
    if pd.isna(xdomain_start) or pd.isna(xdomain_end):
        st.info("表示対象の期間に日付がありません。")
        st.stop()
    if xdomain_start == xdomain_end:
        xdomain_end = xdomain_end + pd.Timedelta(days=1)

    def prob_to_label(v):
        if v is None or pd.isna(v) or v <= 0:
            return "0"
        try:
            return "1/" + str(int(round(1.0 / float(v))))
        except Exception:
            return "0"

    if is_prob_metric:
        df_plot["inv_label"] = df_plot["plot_val"].apply(prob_to_label)
    else:
        df_plot["inv_label"] = df_plot["plot_val"].apply(lambda v: "" if v is None or pd.isna(v) else f"{v:,.0f}")

    if is_prob_metric:
        thresholds = setting_map.get(machine_sel, {})
        if thresholds:
            df_rules = pd.DataFrame([{"setting": k, "value": float(v)} for k, v in thresholds.items()])
        else:
            df_rules = pd.DataFrame(columns=["setting", "value"])
    else:
        df_rules = pd.DataFrame(columns=["setting", "value"])

    legend_sel = alt.selection_point(fields=["setting"], bind="legend")

    if is_prob_metric:
        y_axis = alt.Axis(
            title=metric_col,
            format=".4f",
            labelExpr=(
                "isValid(datum.value) && isFinite(datum.value) "
                "? (datum.value <= 0 ? '0' : '1/' + format(1/datum.value, '.0f')) "
                ": ''"
            ),
        )
    else:
        y_axis = alt.Axis(title=metric_col, format=",.0f")

    x_axis_days = alt.Axis(title="日付", format="%m/%d", labelAngle=0)
    x_scale = alt.Scale(domain=[xdomain_start, xdomain_end])
    x_field = alt.X("date:T", axis=x_axis_days, scale=x_scale)

    tooltip_fields = [alt.Tooltip("date:T", title="日付", format="%Y-%m-%d")]
    if is_prob_metric:
        tooltip_fields.append(alt.Tooltip("inv_label:N", title="見かけの確率"))
        tooltip_fields.append(alt.Tooltip("plot_val:Q", title="確率(0〜1)", format=".4f"))
    else:
        tooltip_fields.append(alt.Tooltip("plot_val:Q", title=metric_col, format=",.0f"))

    base = (
        alt.Chart(df_plot)
        .mark_line(point=True)
        .encode(x=x_field, y=alt.Y("plot_val:Q", axis=y_axis), tooltip=tooltip_fields)
        .properties(height=400, width="container")
    )

    if not df_rules.empty:
        rules = (
            alt.Chart(df_rules)
            .mark_rule(strokeDash=[4, 2])
            .encode(
                y="value:Q",
                color=alt.Color("setting:N", legend=alt.Legend(title="設定ライン")),
                opacity=alt.condition(legend_sel, alt.value(1), alt.value(0.15)),
            )
        )
        final_chart = (base + rules).add_params(legend_sel)
    else:
        final_chart = base

    st.subheader(title)
    st.altair_chart(final_chart, use_container_width=True)


# ============================================================
# 🧠 MLデータ作成モード（予測UI付き）
# ============================================================
if mode == MODE_ML:
    st.header("🧠 機械学習 / 時系列基盤モデル用データ作成（＋ 予測UI）")

    # --- テーブル一覧 ---
    try:
        with eng.connect() as conn:
            tables = [r[0] for r in conn.execute(sa.text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'"))]
    except Exception as e:
        st.error(f"テーブル一覧取得エラー: {e}")
        st.stop()

    if not tables:
        st.info("まず取り込みモードでデータを入れてください。")
        st.stop()

    default_table = "slot_プレゴ立川"
    default_index = next((i for i, t in enumerate(tables) if t == default_table), 0)

    table_name = st.selectbox("店舗テーブル（slot_◯◯）", tables, index=default_index, key="ml_table")
    TBL_Q = q(table_name)

    # --- 日付範囲 ---
    @st.cache_data(ttl=600)
    def get_date_range_ml(table_name: str):
        TBL_Q_inner = q(table_name)
        with eng.connect() as conn:
            row = conn.execute(sa.text(f"SELECT MIN(date), MAX(date) FROM {TBL_Q_inner}")).first()
        return (row[0], row[1]) if row else (None, None)

    min_date, max_date = get_date_range_ml(table_name)
    if not (min_date and max_date):
        st.warning("このテーブルに日付データがありません。")
        st.stop()

    c1, c2 = st.columns(2)
    ml_start = c1.date_input("開始日", value=min_date, min_value=min_date, max_value=max_date, key="ml_start")
    ml_end = c2.date_input("終了日", value=max_date, min_value=min_date, max_value=max_date, key="ml_end")

    # --- 機種一覧 ---
    @st.cache_data(ttl=600)
    def get_machines_ml(table_name: str, start: dt.date, end: dt.date):
        TBL_Q_inner = q(table_name)
        sql = sa.text(
            f'SELECT DISTINCT "機種" FROM {TBL_Q_inner} '
            f"WHERE date BETWEEN :s AND :e ORDER BY \"機種\""
        )
        with eng.connect() as conn:
            return [r[0] for r in conn.execute(sql, {"s": start, "e": end})]

    machines = get_machines_ml(table_name, ml_start, ml_end)
    if not machines:
        st.warning("指定期間にデータがありません。")
        st.stop()

    machine_sel = st.selectbox("機種", machines, key="ml_machine")

    # --- 数値カラム候補（DB定義から） ---
    insp = inspect(eng)
    cols_info = insp.get_columns(table_name)

    numeric_candidates: list[str] = []
    for c in cols_info:
        name = c["name"]
        if name in {"date", "機種", "台番号"}:
            continue
        col_type = str(c["type"]).upper()
        if any(t in col_type for t in ("INT", "NUMERIC", "REAL", "DOUBLE", "FLOAT")):
            numeric_candidates.append(name)

    if not numeric_candidates:
        st.error("数値カラムが見つかりません。")
        st.stop()

    prob_cols = [c for c in ["合成確率", "BB確率", "RB確率", "ART確率"] if c in numeric_candidates]
    other_cols = [c for c in numeric_candidates if c not in prob_cols]
    numeric_candidates = prob_cols + sorted(other_cols)

    # --- 粒度 ---
    gran = st.radio("粒度", ["台別（台番号ごと）", "全台平均（dateで集約）"], horizontal=True, key="ml_gran")

    # --- 予測タスク（2択） ---
    TASK_SETTING = "① 設定推定（合成確率→setting.json）"
    TASK_PAYOUT = "② 差枚系予測（差枚/差玉/最大差玉/最大持玉）"

    default_task = TASK_SETTING if (machine_sel in setting_map and setting_map.get(machine_sel)) else TASK_PAYOUT
    task = st.radio(
        "予測パターン",
        [TASK_SETTING, TASK_PAYOUT],
        index=[TASK_SETTING, TASK_PAYOUT].index(default_task),
        horizontal=True,
        key="ml_task",
    )

    # --- target 決定 ---
    if task == TASK_SETTING:
        if "合成確率" not in numeric_candidates:
            st.error("このテーブルには『合成確率』が無いので設定推定はできません。差枚系予測を選んでください。")
            st.stop()
        target_col = "合成確率"
        st.caption("合成確率(0〜1)を予測 → 予測値を setting.json の設定ラインに最も近い設定へ割り当てます。")
        if not setting_map.get(machine_sel, {}):
            st.warning("setting.json にこの機種の設定ラインがありません（設定推定のラベル付けができません）。")
    else:
        payout_cands = build_payout_candidates(numeric_candidates)
        if not payout_cands:
            st.warning("差枚/差玉/最大差玉/最大持玉 系が見つからないため、数値カラム先頭をtargetにします。")
            target_col = numeric_candidates[0]
        else:
            labels = [c["label"] for c in payout_cands]
            picked = st.selectbox("target（差枚相当）に使う列", options=labels, index=0, key="ml_payout_target_pick")
            picked_obj = payout_cands[labels.index(picked)]
            target_col = picked_obj["source"]
        st.caption(f"このテーブルでは **{target_col}** を「差枚相当」として予測します（店ごとに列が違うため）。")

    st.write("✅ 今回予測するもの（target）:", target_col)

    # --- 特徴量（共変量）任意 ---
    default_feats = [c for c in ["累計スタート", "スタート回数", "BB回数", "RB回数", "ART回数", "最大持玉", "最大差玉"] if c in numeric_candidates]
    feats = st.multiselect("特徴量（共変量）として付けたいカラム（任意）", numeric_candidates, default=default_feats, key="ml_feats")

    # --- 出力形式（ダウンロードだけに影響。予測UIは常に長形式を内部使用） ---
    out_fmt = st.selectbox(
        "CSVダウンロード形式",
        ["長形式（Chronos-2 / TimesFM向け）", "広形式（timestamp index, series columns）"],
        key="ml_outfmt",
    )

    # --- 台番号（台別のときだけ） ---
    slots_sel: list[int] | None = None
    if gran == "台別（台番号ごと）":
        @st.cache_data(ttl=600)
        def get_slots_ml(table_name: str, machine: str, start: dt.date, end: dt.date):
            TBL_Q_inner = q(table_name)
            sql = sa.text(
                f"""
                SELECT DISTINCT "台番号"
                FROM {TBL_Q_inner}
                WHERE "機種" = :m
                  AND date BETWEEN :s AND :e
                  AND "台番号" IS NOT NULL
                ORDER BY "台番号"
                """
            )
            with eng.connect() as conn:
                vals = [r[0] for r in conn.execute(sql, {"m": machine, "s": start, "e": end})]
            return [int(v) for v in vals if v is not None]

        slots = get_slots_ml(table_name, machine_sel, ml_start, ml_end)
        if not slots:
            st.warning("台番号が見つかりません。")
            st.stop()

        slots_sel = st.multiselect("対象台番号（未選択なら全台）", slots, default=[], key="ml_slots_multi")

    # --- DBから取得（キャッシュ） ---
    @st.cache_data(ttl=300)
    def fetch_ml_df(
        table_name: str,
        machine: str,
        start: dt.date,
        end: dt.date,
        cols: list[str],
        slots: list[int] | None,
        avg: bool,
    ) -> pd.DataFrame:
        TBL_Q_inner = q(table_name)

        slots_clause = ""
        bindparams = []
        params = {"m": machine, "s": start, "e": end}

        if slots is not None and len(slots) > 0:
            slots_clause = ' AND "台番号" IN :slots'
            bindparams.append(sa.bindparam("slots", expanding=True))
            params["slots"] = slots

        if avg:
            agg_cols = ", ".join([f"AVG({q(c)}) AS {q(c)}" for c in cols])
            sql = sa.text(
                f"""
                SELECT
                    date,
                    :m AS "機種",
                    NULL::int AS "台番号",
                    {agg_cols}
                FROM {TBL_Q_inner}
                WHERE "機種" = :m
                  AND date BETWEEN :s AND :e
                  {slots_clause}
                GROUP BY date
                ORDER BY date
                """
            )
        else:
            select_cols = ["date", '"機種"', '"台番号"'] + [q(c) for c in cols]
            sql = sa.text(
                f"""
                SELECT {", ".join(select_cols)}
                FROM {TBL_Q_inner}
                WHERE "機種" = :m
                  AND date BETWEEN :s AND :e
                  {slots_clause}
                ORDER BY date, "台番号"
                """
            )

        if bindparams:
            sql = sql.bindparams(*bindparams)

        with eng.connect() as conn:
            return pd.read_sql(sql, conn, params=params)

    cols_out = list(dict.fromkeys([target_col] + feats))
    avg = (gran == "全台平均（dateで集約）")
    df = fetch_ml_df(table_name, machine_sel, ml_start, ml_end, cols_out, slots_sel, avg)

    if df.empty:
        st.warning("この条件でデータがありません。")
        st.stop()

    # --- series id（系列ID） ---
    def make_id(row):
        slot = row["台番号"]
        if pd.isna(slot):
            return f"{table_name}|{row['機種']}|AVG"
        return f"{table_name}|{row['機種']}|{int(slot)}"

    df = df.copy()
    df["id"] = df.apply(make_id, axis=1)
    df["timestamp"] = pd.to_datetime(df["date"])

    # --- 長形式（予測UI用） ---
    out_long = df.rename(columns={target_col: "target"}).copy()
    keep_cols = ["id", "timestamp", "target"] + [c for c in feats if c in out_long.columns]
    out_long = out_long[keep_cols].sort_values(["id", "timestamp"])

    # --- 広形式（ダウンロード用） ---
    out_wide = df.pivot_table(index="timestamp", columns="id", values=target_col, aggfunc="mean").sort_index()

    # --- プレビュー & ダウンロード ---
    st.subheader("📦 データ出力（プレビュー）")
    if out_fmt.startswith("長形式"):
        st.dataframe(out_long.head(50), use_container_width=True)
        st.download_button(
            "⬇️ CSVダウンロード（長形式）",
            data=out_long.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
            file_name=safe_filename(f"ml_long_{table_name}_{machine_sel}_{ml_start}_{ml_end}.csv"),
            mime="text/csv",
        )
        st.caption("Chronos-2 / TimesFM向け（id,timestamp,target）")
    else:
        st.dataframe(out_wide.head(50), use_container_width=True)
        st.download_button(
            "⬇️ CSVダウンロード（広形式）",
            data=out_wide.to_csv(index=True, encoding="utf-8-sig").encode("utf-8-sig"),
            file_name=safe_filename(f"ml_wide_{table_name}_{machine_sel}_{ml_start}_{ml_end}.csv"),
            mime="text/csv",
        )
        st.caption("wide形式（timestamp index, series columns）")

    # ============================================================
    # 🔮 予測をUIで実行（CLI不要）
    # ============================================================
    st.divider()
    st.subheader("🔮 時系列基盤モデルで予測（UI実行）")

    uniq_ids = out_long["id"].unique().tolist()
    st.caption(f"系列数: {len(uniq_ids)}（多いと重いので、まずは少数で試すのがおすすめ）")

    max_n = min(200, len(uniq_ids))
    n_pick = st.slider(
        "候補に出す系列数（先頭から）",
        1,
        max_n if max_n >= 1 else 1,
        min(20, max_n) if max_n >= 1 else 1,
        key="fcst_topn",
    )
    cand_ids = uniq_ids[:n_pick]

    pick_ids = st.multiselect("予測する系列（id）", options=cand_ids, default=cand_ids[:1], key="fcst_ids")
    if not pick_ids:
        st.warning("少なくとも1つ選んでください。")
        st.stop()

    df_long_use = out_long[out_long["id"].isin(pick_ids)].copy()

    c1, c2, c3, c4 = st.columns(4)
    model_name = c1.selectbox("モデル", ["chronos2", "timesfm"], index=0, key="fcst_model")
    horizon = c2.slider("予測ホライズン（日数）", 1, 60, 14, key="fcst_h")
    device_map = c3.selectbox("デバイス（Chronos-2）", ["cpu", "cuda"], index=0, key="fcst_dev")
    freq = c4.selectbox("freq（TimesFM）", ["D", "W", "M"], index=0, key="fcst_freq")

    if st.button("🚀 予測を実行", key="run_forecast"):
        try:
            with st.spinner("モデルを準備して予測中…（初回は重いです）"):
                if model_name == "chronos2":
                    pred = forecast_with_chronos2(
                        df_long_use[["id", "timestamp", "target"]],
                        horizon=horizon,
                        device_map=device_map,
                    )
                else:
                    pred = forecast_with_timesfm(
                        df_long_use[["id", "timestamp", "target"]],
                        horizon=horizon,
                        freq=freq,
                    )

            # 設定推定（合成確率の場合だけ）
            if task == TASK_SETTING:
                thresholds = setting_map.get(machine_sel, {})
                if thresholds:
                    pred = pred.copy()
                    pred["pred_setting"] = pred["yhat"].apply(lambda p: score_setting_by_denom(p, thresholds))
                    pred["pred_1_over"] = pred["yhat"].apply(
                        lambda p: 0 if (p is None or (not np.isfinite(p)) or p <= 0) else int(round(1.0 / p))
                    )
                else:
                    pred = pred.copy()
                    pred["pred_setting"] = None
                    pred["pred_1_over"] = None

            # ============================================================
            # ✅ 予測結果を「わかりやすく表示」
            # ============================================================
            pred_view = pred.copy()
            pred_view["timestamp"] = pd.to_datetime(pred_view["timestamp"])

            if task == TASK_SETTING:
                pred_view["yhat_denom"] = pred_view["yhat"].apply(
                    lambda p: np.nan if (p is None or (not np.isfinite(p)) or p <= 0) else round(1.0 / float(p))
                )
                pred_view["yhat_disp"] = pred_view["yhat_denom"].apply(lambda d: "—" if pd.isna(d) else f"1/{int(d)}")
            else:
                pred_view["yhat_disp"] = pred_view["yhat"].apply(
                    lambda v: "—" if (v is None or pd.isna(v)) else f"{int(round(float(v))):,}"
                )

            hist = df_long_use[["id", "timestamp", "target"]].copy()
            hist["timestamp"] = pd.to_datetime(hist["timestamp"])
            hist = hist.sort_values(["id", "timestamp"])

            st.success("予測完了！")
            st.subheader("📌 予測結果（見やすい表示）")

            vmode = st.radio("表示", ["グラフ中心", "表中心", "両方"], horizontal=True, index=2, key="pred_view_mode")
            show_band = st.checkbox("不確実性の帯を表示（Chronos-2の0.1/0.9がある場合）", value=True, key="pred_show_band")
            hist_days = st.slider("実績を何日分重ねて表示する？", 7, 90, 30, step=1, key="pred_hist_days")

            view_ids = pred_view["id"].unique().tolist()
            tabs = st.tabs([f"🧩 {i}" for i in view_ids])

            for ti, _id in enumerate(view_ids):
                with tabs[ti]:
                    p1 = pred_view[pred_view["id"] == _id].sort_values("timestamp").copy()
                    h1 = hist[hist["id"] == _id].sort_values("timestamp").copy()

                    if not h1.empty:
                        last_ts = h1["timestamp"].max()
                        h1 = h1[h1["timestamp"] >= (last_ts - pd.Timedelta(days=hist_days))].copy()

                    # ---- サマリー ----
                    cA, cB, cC, cD = st.columns(4)
                    next_row = p1.iloc[0] if len(p1) > 0 else None

                    if task == TASK_SETTING:
                        next_disp = next_row["yhat_disp"] if next_row is not None else "—"
                        next_set = next_row.get("pred_setting", "—") if next_row is not None else "—"
                        cA.metric("次の日の予測（合成）", next_disp)
                        cB.metric("次の日の予測設定", str(next_set))
                    else:
                        next_disp = next_row["yhat_disp"] if next_row is not None else "—"
                        cA.metric(f"次の日の予測（{target_col}）", next_disp)
                        cB.metric("（空）", "")

                    if not p1.empty:
                        avg_val = float(p1["yhat"].mean())
                        if task == TASK_SETTING:
                            avg_disp = "—" if avg_val <= 0 else f"1/{int(round(1/avg_val))}"
                        else:
                            avg_disp = f"{int(round(avg_val)):,}"
                    else:
                        avg_disp = "—"
                    cC.metric("予測期間の平均", avg_disp)

                    if len(p1) >= 2:
                        slope = float(p1["yhat"].iloc[-1] - p1["yhat"].iloc[0])
                        if task == TASK_SETTING:
                            d0 = p1["yhat_denom"].iloc[0] if "yhat_denom" in p1.columns else np.nan
                            d1 = p1["yhat_denom"].iloc[-1] if "yhat_denom" in p1.columns else np.nan
                            slope_disp = "—" if (pd.isna(d0) or pd.isna(d1)) else f"{int(d1 - d0):+d} (分母差)"
                        else:
                            slope_disp = f"{int(round(slope)):+,}"
                    else:
                        slope_disp = "—"
                    cD.metric("期間の変化量（ざっくり）", slope_disp)

                    # ---- グラフ（実績＋予測）----
                    if vmode in ("グラフ中心", "両方"):
                        chart_hist = (
                            alt.Chart(h1)
                            .mark_line(point=True)
                            .encode(
                                x=alt.X("timestamp:T", title="日付"),
                                y=alt.Y("target:Q", title=f"実績（{target_col}）"),
                                tooltip=[
                                    alt.Tooltip("timestamp:T", title="日付", format="%Y-%m-%d"),
                                    alt.Tooltip("target:Q", title="実績", format=".6f" if task == TASK_SETTING else ",.0f"),
                                ],
                            )
                        )

                        chart_pred = (
                            alt.Chart(p1)
                            .mark_line(point=True, strokeDash=[4, 2])
                            .encode(
                                x=alt.X("timestamp:T", title="日付"),
                                y=alt.Y("yhat:Q", title=f"予測（{target_col}）"),
                                tooltip=[
                                    alt.Tooltip("timestamp:T", title="日付", format="%Y-%m-%d"),
                                    alt.Tooltip("yhat_disp:N", title="予測(表示用)"),
                                    alt.Tooltip("yhat:Q", title="予測(数値)", format=".6f" if task == TASK_SETTING else ",.0f"),
                                ],
                            )
                        )

                        band = None
                        if show_band and ("0.1" in p1.columns) and ("0.9" in p1.columns):
                            band = (
                                alt.Chart(p1)
                                .mark_area(opacity=0.2)
                                .encode(
                                    x="timestamp:T",
                                    y=alt.Y("0.1:Q", title=""),
                                    y2="0.9:Q",
                                    tooltip=[
                                        alt.Tooltip("timestamp:T", title="日付", format="%Y-%m-%d"),
                                        alt.Tooltip("0.1:Q", title="下振れ(0.1)", format=".6f"),
                                        alt.Tooltip("0.9:Q", title="上振れ(0.9)", format=".6f"),
                                    ],
                                )
                            )

                        final = (chart_hist + band + chart_pred) if band is not None else (chart_hist + chart_pred)
                        st.altair_chart(final.properties(height=320), use_container_width=True)

                    # ---- 表（読みやすく）----
                    if vmode in ("表中心", "両方"):
                        show_cols = ["timestamp", "yhat_disp"]
                        rename_map = {"timestamp": "日付", "yhat_disp": "予測値"}

                        if task == TASK_SETTING:
                            if "pred_setting" in p1.columns:
                                show_cols += ["pred_setting"]
                                rename_map["pred_setting"] = "予測設定"
                            show_cols += ["yhat"]
                            rename_map["yhat"] = "予測(確率0-1)"
                        else:
                            show_cols += ["yhat"]
                            rename_map["yhat"] = f"予測({target_col})"

                        tdf = p1[show_cols].copy().rename(columns=rename_map)
                        st.dataframe(tdf, use_container_width=True, height=260)

                        light = p1[["timestamp", "yhat_disp"]].copy()
                        light = light.rename(columns={"timestamp": "date", "yhat_disp": "prediction"})
                        st.download_button(
                            "⬇️ この台だけの軽量CSV（date,prediction）",
                            data=light.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                            file_name=safe_filename(f"pred_light_{model_name}_{_id}.csv"),
                            mime="text/csv",
                            key=f"dl_light_{_id}",
                        )

            # 全体CSV
            fname = safe_filename(
                f"pred_{model_name}_{'setting' if task==TASK_SETTING else 'payout'}_{table_name}_{machine_sel}_{ml_start}_{ml_end}.csv"
            )
            st.download_button(
                "⬇️ 予測結果CSV（全体）をダウンロード",
                data=pred.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig"),
                file_name=fname,
                mime="text/csv",
                key="dl_pred_all",
            )

        except ModuleNotFoundError as e:
            st.error(
                "必要ライブラリが入っていません。\n"
                "requirements.txt に torch / transformers / accelerate / chronos-forecasting / timesfm を追加して再デプロイしてください。\n"
                f"詳細: {e}"
            )
        except Exception as e:
            st.error(f"予測実行でエラー: {e}")
