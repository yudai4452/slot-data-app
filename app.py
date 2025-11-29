import io
import re
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
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from uuid import uuid4

# ======================== 設定読み込み ========================
@st.cache_data
def load_settings() -> dict:
    with open("setting.json", "r", encoding="utf-8") as f:
        return json.load(f)

SETTINGS = load_settings()
PG_CFG = SETTINGS["connections"]["slot_db"]
SA_INFO = SETTINGS["gcp_service_account"]

# ======================== Drive / DB 接続 ========================
def make_drive():
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
    return make_drive()

drive = gdrive()

@st.cache_resource
def engine():
    try:
        url = (
            f"postgresql+psycopg2://{PG_CFG['username']}:{PG_CFG['password']}"
            f"@{PG_CFG['host']}:{PG_CFG['port']}/{PG_CFG['database']}?sslmode={PG_CFG.get('sslmode', 'require')}"
        )
        return sa.create_engine(url, pool_pre_ping=True)
    except Exception as e:
        st.error(f"DB接続エラー: {e}")
        return None

eng = engine()
if eng is None:
    st.stop()

# ======================== Google Drive 検索 ========================
def list_csv_recursive(folder_id: str) -> list[dict]:
    """指定フォルダ以下の csv を全部列挙する（仮想 path 付き）"""
    if drive is None:
        st.error("Drive クライアントを初期化できていません。")
        st.stop()

    stack = [(folder_id, "")]
    files: list[dict] = []

    while stack:
        fid, base = stack.pop()
        q = f"'{fid}' in parents and trashed=false"
        page_token = None
        while True:
            resp = drive.files().list(
                q=q,
                pageSize=1000,
                fields="nextPageToken, files(id, name, mimeType, md5Checksum)",
                pageToken=page_token,
            ).execute()
            for f in resp.get("files", []):
                name = f["name"]
                mime = f["mimeType"]
                if mime == "application/vnd.google-apps.folder":
                    stack.append((f["id"], base + name + "/"))
                    continue
                if not name.lower().endswith(".csv"):
                    continue
                files.append(
                    {
                        "id": f["id"],
                        "name": name,
                        "path": base + name,
                        "mimeType": mime,
                        "md5Checksum": f.get("md5Checksum") or "",
                    }
                )
            page_token = resp.get("nextPageToken")
            if not page_token:
                break
    return files

# ======================== メタ情報抽出 ========================
def parse_meta(path: str) -> tuple[str, str, dt.date]:
    """
    path から 店舗名, 機種名, 日付 を抽出する。
    例: "プレゴ立川/マイジャグラーⅤ/slot_machine_data_2025-01-01.csv"
    """
    parts = path.split("/")
    if len(parts) < 3:
        raise ValueError(f"パス形式が想定外です: {path}")

    store = parts[0]
    machine = parts[1]

    m = re.search(r"(\d{4}-\d{2}-\d{2})", parts[-1])
    if not m:
        raise ValueError(f"ファイル名に日付(YYYY-MM-DD)が見つかりません: {parts[-1]}")
    date = dt.datetime.strptime(m.group(1), "%Y-%m-%d").date()

    return store, machine, date

# ======================== カラムマッピング ========================
COLUMN_MAP = {
    "メッセ武蔵境": {
        "台番号": "台番号",
        "スタート回数": "スタート回数",
        "累計スタート": "累計スタート",
        "BB回数": "BB回数",
        "RB回数": "RB回数",
        "ART回数": "ART回数",
        "最大持ち玉": "最大持玉",
    },
    "プレゴ立川": {
        "台番号": "台番号",
        "総スタート": "累計スタート",
        "BB回数": "BB回数",
        "RB回数": "RB回数",
        "AT/ART回数": "ART回数",
        "最大持玉": "最大持玉",
    },
    "ジャンジャンマールゴット分倍河原": {
        "台番号": "台番号",
        "スタート": "スタート回数",
        "累計スタート": "累計スタート",
        "BB回数": "BB回数",
        "RB回数": "RB回数",
        "ART回数": "ART回数",
        "最大持玉": "最大持玉",
    },
}

def normalize(df_raw: pd.DataFrame, store: str) -> pd.DataFrame:
    if store not in COLUMN_MAP:
        raise ValueError(f"未対応店舗: {store}")

    mapping = COLUMN_MAP[store]

    df = df_raw.rename(columns=mapping).copy()

    required = ["台番号", "累計スタート", "BB回数", "RB回数", "ART回数", "最大持玉"]
    for col in required:
        if col not in df.columns:
            df[col] = 0

    def safe_int(x):
        try:
            if pd.isna(x):
                return 0
            s = str(x).replace(",", "").replace(" ", "").strip()
            if s == "":
                return 0
            return int(float(s))
        except Exception:
            return 0

    for col in required:
        df[col] = df[col].map(safe_int)

    return df[required]

def load_and_normalize(raw_bytes: bytes, store: str) -> pd.DataFrame:
    """
    エンコーディングを shift_jis 固定で読み、カラムを正規化。
    """
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

# ======================== import_log（差分取り込み） ========================
def ensure_import_log_table():
    meta = sa.MetaData()
    insp = inspect(eng)
    if not insp.has_table("import_log"):
        t = sa.Table(
            "import_log",
            meta,
            sa.Column("file_id", sa.String(128), primary_key=True),
            sa.Column("md5", sa.String(64), nullable=False),
            sa.Column("path", sa.String(512), nullable=False),
            sa.Column("store", sa.String(128), nullable=False),
            sa.Column("machine", sa.String(128), nullable=False),
            sa.Column("date", sa.Date, nullable=False),
            sa.Column("rows", sa.Integer, nullable=False),
            sa.Column("imported_at", sa.DateTime(timezone=True), nullable=False),
        )
        meta.create_all(eng)
        return t
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
            "imported_at": stmt.excluded.imported_at,
        },
    )
    with eng.begin() as conn:
        conn.execute(stmt)

# ======================== STORE テーブル ========================
def ensure_store_table(store: str) -> sa.Table:
    """
    店舗単位で 1 テーブル：
    slot_プレゴ立川
    """
    table_name = "slot_" + store.replace(" ", "_")
    meta = sa.MetaData()
    insp = inspect(eng)
    if insp.has_table(table_name):
        return sa.Table(table_name, meta, autoload_with=eng)

    tbl = sa.Table(
        table_name,
        meta,
        sa.Column("date", sa.Date, nullable=False),
        sa.Column("機種", sa.String(128), nullable=False),
        sa.Column("台番号", sa.Integer, nullable=False),
        sa.Column("累計スタート", sa.Integer, nullable=False),
        sa.Column("BB回数", sa.Integer, nullable=False),
        sa.Column("RB回数", sa.Integer, nullable=False),
        sa.Column("ART回数", sa.Integer, nullable=False),
        sa.Column("最大持玉", sa.Integer, nullable=False),
        sa.PrimaryKeyConstraint("date", "機種", "台番号", name=f"pk_{table_name}"),
    )
    meta.create_all(eng)
    return tbl

# ======================== 通常 UPSERT ========================
def upsert_dataframe(conn, table: sa.Table, df: pd.DataFrame):
    if df.empty:
        return

    ins = pg_insert(table)
    update_cols = [c.name for c in table.columns if c.name not in ("date", "機種", "台番号")]
    stmt = ins.on_conflict_do_update(
        index_elements=["date", "機種", "台番号"],
        set_={c: ins.excluded[c] for c in update_cols},
    )
    conn.execute(stmt, df.to_dict(orient="records"))

# ======================== COPY 高速化 ========================
def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def bulk_upsert_copy_merge(engine: sa.Engine, table: sa.Table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
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

    create_tmp_sql = f'CREATE TEMP TABLE {q(tmp_name)} (LIKE {q(table.name)} INCLUDING ALL);'
    copy_sql = f'COPY {q(tmp_name)} ({cols_q}) FROM STDIN WITH (FORMAT csv, HEADER true);'
    insert_sql = (
        f'INSERT INTO {q(table.name)} ({cols_q}) SELECT {cols_q} FROM {q(tmp_name)} '
        f'ON CONFLICT ({pk_q}) DO ' + ('NOTHING;' if not set_clause else f'UPDATE SET {set_clause};')
    )
    drop_tmp_sql = f'DROP TABLE IF EXISTS {q(tmp_name)};'

    with engine.begin() as conn:
        driver_conn = getattr(conn.connection, "driver_connection", None)
        if driver_conn is None:
            driver_conn = conn.connection.connection  # fallback psycopg2 connection

        with driver_conn.cursor() as cur:
            cur.execute(create_tmp_sql)
            cur.copy_expert(copy_sql, io.StringIO(csv_text))
            cur.execute(insert_sql)
            cur.execute(drop_tmp_sql)

# ======================== 並列処理：1ファイル ========================
def process_one_file(file_meta: dict) -> dict | None:
    try:
        store, machine, date = parse_meta(file_meta["path"])
        if store not in COLUMN_MAP:
            # 未対応店舗はエラーとして記録してスキップ
            return {"error": f"{file_meta.get('path','(unknown)')} 未対応店舗のためスキップ: {store}"}

        if drive is None:
            # 起動時に既にエラーを出して止めている想定だが、念のためセーフガード
            raise RuntimeError("Drive クライアントの初期化に失敗しています。")

        raw = drive.files().get_media(fileId=file_meta["id"]).execute()
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
        return {"error": f"{file_meta.get('path','(unknown)')} 処理エラー: {e}"}

# ======================== 自動バッチ実行ヘルパー ========================
def run_import_for_targets(targets: list[dict], workers: int, use_copy: bool):
    detail_status = st.empty()
    created_tables: dict[str, sa.Table] = {}
    import_log_entries = []
    errors = []
    bucket: dict[str, list[dict]] = defaultdict(list)

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
            detail_status.text(f"処理完了: {res['path']}")

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
                bulk_upsert_copy_merge(eng, tbl, df_all)
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
                    "imported_at": dt.datetime.now(dt.timezone.utc),
                }
            )

    processed_files = sum(len(v) for v in bucket.values())
    return import_log_entries, errors, processed_files

# ========================= Streamlit UI =========================
st.set_page_config(page_title="Slot Data Importer", layout="wide")

mode = st.sidebar.radio("モード選択", ["📥 データ取り込み", "📊 可視化"])

# ========================= 取り込みモード =========================
if mode == "📥 データ取り込み":
    st.header("Google Drive → Postgres インポート")

    folder_options = {
        "🧪 テスト用": SETTINGS["import_folders"]["test"],
        "🚀 本番用": SETTINGS["import_folders"]["prod"],
    }
    options = list(folder_options.keys())
    default_idx = options.index("🚀 本番用") if "🚀 本番用" in options else 0
    sel_label = st.selectbox("対象フォルダ", options, index=default_idx, key="folder_type")
    folder_id = folder_options[sel_label]

    col1, col2 = st.columns(2)
    with col1:
        imp_start = st.date_input("インポート対象開始日", dt.date.today() - dt.timedelta(days=7))
    with col2:
        imp_end = st.date_input("インポート対象終了日", dt.date.today())

    if imp_start > imp_end:
        st.error("開始日は終了日以前にしてください")
        st.stop()

    c1, c2, c3 = st.columns(3)
    workers = c1.slider("並列ダウンロード数", 1, 16, 4)
    use_copy = c2.checkbox(
        "DB書き込みをCOPYで高速化（推奨）",
        value=True,
        help="一時テーブルにCOPY→まとめてUPSERT。失敗時は通常UPSERTに自動フォールバック。",
        key="use_copy",
    )
    max_files = c3.slider(
        "最大ファイル数（1回の実行上限）",
        10,
        2000,
        300,
        step=10,
        help="大量フォルダは分割して取り込み（タイムアウト回避）",
        key="max_files",
    )

    auto_batch = st.checkbox("最大ファイル数ごとに自動で続きのバッチも実行する", value=False, key="auto_batch")
    max_batches = st.number_input(
        "最大バッチ回数（0で制限なし）",
        min_value=0,
        max_value=100,
        value=3,
        help="実行時間が長くなりすぎるのを防ぐための上限。0なら全バッチ実行。",
        key="max_batches",
    )

    if st.button("🚀 インポート実行", disabled=not folder_id, key="import_run"):
        try:
            files_all = list_csv_recursive(folder_id)
            files: list[dict] = []
            skipped: list[str] = []
            for f in files_all:
                try:
                    _, _, file_date = parse_meta(f["path"])
                except ValueError as e:
                    skipped.append(f"{f['path']}: {e}")
                    continue
                if imp_start <= file_date <= imp_end:
                    files.append(f)
        except Exception as e:
            st.error(f"ファイル一覧取得エラー: {e}")
            st.stop()

        if skipped:
            st.info(f"パス形式が想定外でスキップしたファイルが {len(skipped)} 件あります。")

        imported_md5 = get_imported_md5_map()
        all_targets = [
            f for f in files if imported_md5.get(f["id"], "") != (f.get("md5Checksum") or "")
        ]
        if not all_targets:
            st.success("差分はありません（すべて最新）")
            st.stop()

        all_targets.sort(key=lambda f: parse_meta(f["path"])[2])

        batches = [all_targets[i : i + max_files] for i in range(0, len(all_targets), max_files)]

        # 実際に処理するバッチ一覧
        if not auto_batch:
            # 自動バッチ OFF → 1バッチだけ
            use_batches = batches[:1]
        else:
            # 自動バッチ ON → max_batches が 0 なら全バッチ、それ以外は指定数まで
            if max_batches == 0:
                use_batches = batches
            else:
                use_batches = batches[: int(max_batches)]

        total_files = sum(len(b) for b in use_batches)
        done_files = 0
        bar = st.progress(0.0)
        batch_status = st.empty()
        all_errors: list[str] = []

        for bi, batch in enumerate(use_batches, start=1):
            batch_status.text(f"バッチ {bi}/{len(use_batches)}（{len(batch)} 件）を処理中…")
            entries, errors, processed_files = run_import_for_targets(batch, workers, use_copy)
            upsert_import_log(entries)
            all_errors.extend(errors)

            done_files += processed_files
            bar.progress(min(1.0, done_files / max(1, total_files)))

        batch_status.text("")

        # まだ残りがある場合だけ案内メッセージ
        if auto_batch and max_batches > 0 and len(batches) > len(use_batches):
            remaining = sum(len(b) for b in batches[len(use_batches) :])
            st.info(
                f"最大バッチ回数に達しました。残り {remaining} 件は、再度ボタンを押すと続きから処理します。"
            )

        if all_errors:
            st.warning("一部でエラーが発生しました。詳細：")
            for msg in all_errors[:50]:
                st.write("- " + msg)
            if len(all_errors) > 50:
                st.write(f"... ほか {len(all_errors)-50} 件")

        st.success(f"インポート完了（処理ファイル: {done_files} 件）！")

# ========================= 可視化モード =========================
if mode == "📊 可視化":
    st.header("DB 可視化")

    # 1) テーブル一覧
    try:
        with eng.connect() as conn:
            tables = [
                r[0]
                for r in conn.execute(
                    sa.text("SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'")
                )
            ]
    except Exception as e:
        st.error(f"テーブル一覧取得エラー: {e}")
        st.stop()

    if not tables:
        st.info("まず取り込みモードでデータを入れてください。")
        st.stop()

    table_name = st.selectbox("テーブルを選択", tables)
    if not table_name:
        st.error("テーブルが選択されていません")
        st.stop()

    TBL_Q = '"' + table_name.replace('"', '""') + '"'

    # 2) 最小/最大日付（キャッシュ）
    @st.cache_data(ttl=600)
    def get_date_range(table_name: str):
        TBL_Q = '"' + table_name.replace('"', '""') + '"'
        with eng.connect() as conn:
            row = conn.execute(sa.text(f"SELECT MIN(date), MAX(date) FROM {TBL_Q}")).first()
        return (row[0], row[1]) if row else (None, None)

    min_date, max_date = get_date_range(table_name)
    if not (min_date and max_date):
        st.info("このテーブルには日付データがありません。まず取り込みを実行してください。")
        st.stop()

    c1, c2 = st.columns(2)
    vis_start = c1.date_input(
        "開始日",
        value=min_date,
        min_value=min_date,
        max_value=max_date,
        key=f"visual_start_{table_name}",
    )
    vis_end = c2.date_input(
        "終了日",
        value=max_date,
        min_value=min_date,
        max_value=max_date,
        key=f"visual_end_{table_name}",
    )

    if vis_start > vis_end:
        st.error("開始日は終了日以前にしてください")
        st.stop()

    # 3) インデックス（任意）
    idx_ok = st.checkbox(
        "読み込み高速化のためのインデックスを作成（推奨・一度だけ）", value=True, key="create_index"
    )
    if idx_ok:
        try:
            with eng.begin() as conn:
                conn.execute(
                    sa.text(
                        f'CREATE INDEX IF NOT EXISTS {table_name}_ix_machine_date ON {TBL_Q} ("機種","date");'
                    )
                )
                conn.execute(
                    sa.text(
                        f'CREATE INDEX IF NOT EXISTS {table_name}_ix_machine_slot_date ON {TBL_Q} ("機種","台番号","date");'
                    )
                )
        except Exception as e:
            st.warning(f"インデックス作成に失敗しました（処理自体は続行します）: {e}")

    # 4) データ取得
    try:
        with eng.connect() as conn:
            df = pd.read_sql(
                sa.text(
                    f"SELECT * FROM {TBL_Q} WHERE date BETWEEN :start AND :end ORDER BY date, 台番号"
                ),
                conn,
                params={"start": vis_start, "end": vis_end},
            )
    except Exception as e:
        st.error(f"データ取得エラー: {e}")
        st.stop()

    if df.empty:
        st.info("該当期間のデータがありません。")
        st.stop()

    with st.expander("生データを見る"):
        st.dataframe(df)

    # ===== 可視化：機種・台番号ごとの成績 =====
    st.subheader("日別集計（BB/RB/ART/スタート）")

    agg = (
        df.groupby("date", as_index=False)[["累計スタート", "BB回数", "RB回数", "ART回数", "最大持玉"]]
        .sum()
        .sort_values("date")
    )
    c1, c2 = st.columns(2)
    with c1:
        st.dataframe(agg)

    with c2:
        chart = (
            alt.Chart(agg)
            .mark_line(point=True)
            .encode(
                x="date:T",
                y=alt.Y("累計スタート:Q", title="累計スタート"),
                tooltip=[
                    "date:T",
                    "累計スタート:Q",
                    "BB回数:Q",
                    "RB回数:Q",
                    "ART回数:Q",
                    "最大持玉:Q",
                ],
            )
            .properties(width="container", height=280)
        )
        st.altair_chart(chart, use_container_width=True)

    # ===== ヒートマップ：台番号 × 日付（最大持玉） =====
    st.subheader("台番号 × 日付 ヒートマップ（最大持玉）")
    heat_df = df[["date", "台番号", "最大持玉"]].copy()

    heat = (
        alt.Chart(heat_df)
        .mark_rect()
        .encode(
            x=alt.X("date:T", title="日付"),
            y=alt.Y("台番号:O", title="台番号"),
            color=alt.Color("最大持玉:Q", title="最大持玉"),
            tooltip=["date:T", "台番号:O", "最大持玉:Q"],
        )
        .properties(width="container", height=400)
    )
    st.altair_chart(heat, use_container_width=True)

    # ===== 単一台の詳細推移 =====
    st.subheader("単一台の詳細推移")
    tai_list = sorted(df["台番号"].unique())
    tai_sel = st.selectbox("台番号を選択", ["（選択なし）"] + [str(t) for t in tai_list])

    if tai_sel != "（選択なし）":
        tai_num = int(tai_sel)
        df_one = df[df["台番号"] == tai_num].sort_values("date")

        st.write(f"台番号 {tai_num} の推移")

        base = alt.Chart(df_one).encode(x="date:T")

        line_start = (
            base.mark_line(point=True)
            .encode(
                y=alt.Y("累計スタート:Q", title="累計スタート"),
                tooltip=[
                    "date:T",
                    "累計スタート:Q",
                    "BB回数:Q",
                    "RB回数:Q",
                    "ART回数:Q",
                    "最大持玉:Q",
                ],
            )
            .properties(width="container", height=240)
        )
        st.altair_chart(line_start, use_container_width=True)

        line_max = (
            base.mark_line(point=True)
            .encode(
                y=alt.Y("最大持玉:Q", title="最大持玉"),
                tooltip=["date:T", "最大持玉:Q"],
            )
            .properties(width="container", height=240)
        )
        st.altair_chart(line_max, use_container_width=True)

    st.caption("※ 可視化は必要に応じてカスタマイズしてOKです。")
