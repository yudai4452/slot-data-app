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

# ======================== 基本設定 ========================
st.set_page_config(page_title="Slot Manager", layout="wide")
mode = st.sidebar.radio("モード", ("📥 データ取り込み", "📊 可視化"))
st.title("🎰 Slot Data Manager & Visualizer")

SA_INFO = st.secrets["gcp_service_account"]
PG_CFG  = st.secrets["connections"]["slot_db"]

# ======================== 設定ファイル ========================
with open("setting.json", encoding="utf-8") as f:
    setting_map = json.load(f)

# ======================== 接続 ========================
def make_drive():
    try:
        creds = Credentials.from_service_account_info(
            SA_INFO, scopes=["https://www.googleapis.com/auth/drive.readonly"]
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
COLUMN_MAP = {
    "メッセ武蔵境": {
        "台番号":"台番号","スタート回数":"スタート回数","累計スタート":"累計スタート",
        "BB回数":"BB回数","RB回数":"RB回数","ART回数":"ART回数",
        "最大持ち玉":"最大持玉","最大持玉":"最大持玉",
        "BB確率":"BB確率","RB確率":"RB確率","ART確率":"ART確率","合成確率":"合成確率",
        "前日最終スタート":"前日最終スタート",
    },
    "ジャンジャンマールゴット分倍河原": {
        "台番号":"台番号","累計スタート":"累計スタート","BB回数":"BB回数","RB回数":"RB回数",
        "最大持ち玉":"最大持玉","最大持玉":"最大持玉",
        "BB確率":"BB確率","RB確率":"RB確率","合成確率":"合成確率",
        "前日最終スタート":"前日最終スタート","スタート回数":"スタート回数",
    },
    "プレゴ立川": {
        "台番号":"台番号","累計スタート":"累計スタート","BB回数":"BB回数","RB回数":"RB回数",
        "最大差玉":"最大差玉","BB確率":"BB確率","RB確率":"RB確率","合成確率":"合成確率",
        "前日最終スタート":"前日最終スタート","スタート回数":"スタート回数",
    },
}

# ======================== Drive: 再帰 + ページング ========================
@st.cache_data
def list_csv_recursive(folder_id: str):
    if drive is None:
        raise RuntimeError("Drive未接続です")
    all_files, queue = [], [(folder_id, "")]
    while queue:
        fid, cur = queue.pop()
        page_token = None
        while True:
            res = drive.files().list(
                q=f"'{fid}' in parents and trashed=false",
                fields="nextPageToken, files(id,name,mimeType,md5Checksum,modifiedTime,size)",
                pageSize=1000, pageToken=page_token
            ).execute()
            for f in res.get("files", []):
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    queue.append((f["id"], f"{cur}/{f['name']}"))
                elif f["name"].lower().endswith(".csv"):
                    all_files.append({**f, "path": f"{cur}/{f['name']}"})
            page_token = res.get("nextPageToken")
            if not page_token:
                break
    return all_files

# ======================== メタ情報解析 ========================
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
            val[(denom <= 0) | (~denom.notna())] = 0
            df.loc[mask_div, col] = val

        # 数値直書き（>1 は 1/値, <=1 はそのまま）
        num = pd.to_numeric(ser[ ~mask_div ], errors="coerce")
        conv = num.copy()
        conv[num > 1] = 1.0 / num[num > 1]
        conv = conv.fillna(0)
        df.loc[~mask_div, col] = conv

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

    int_cols = [
        "台番号", "累計スタート", "スタート回数", "BB回数",
        "RB回数", "ART回数", "最大持玉", "最大差玉", "前日最終スタート"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

    return df

# ======================== 読み込み + 正規化 ========================
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

# ======================== import_log（差分取り込み用） ========================
def ensure_import_log_table():
    meta = sa.MetaData()
    insp = inspect(eng)
    if not insp.has_table("import_log"):
        t = sa.Table(
            "import_log", meta,
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
        set_={"md5": stmt.excluded.md5,
              "path": stmt.excluded.path,
              "store": stmt.excluded.store,
              "machine": stmt.excluded.machine,
              "date": stmt.excluded.date,
              "rows": stmt.excluded.rows,
              "imported_at": sa.func.now()}
    )
    with eng.begin() as conn:
        conn.execute(stmt)

# ======================== テーブル作成 ========================
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

# ======================== 通常UPSERT ========================
def upsert_dataframe(conn, table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
    rows = df.to_dict(orient="records")
    if not rows:
        return
    stmt = pg_insert(table).values(rows)
    update_cols = {c.name: stmt.excluded[c.name] for c in table.c if c.name not in pk}
    stmt = stmt.on_conflict_do_update(index_elements=list(pk), set_=update_cols)
    conn.execute(stmt)

# ======================== COPY→MERGE 高速アップサート ========================
def q(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'

def bulk_upsert_copy_merge(table: sa.Table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
    """
    dfの列だけを対象に TEMP TABLE へ COPY → target に INSERT ... ON CONFLICT DO UPDATE
    """
    if df.empty:
        return

    # ターゲットに存在する列だけ使う（順序も固定）
    valid_cols = [c.name for c in table.c]
    # 欲しい列 = dfに存在し、かつターゲットにも存在
    cols = [c for c in df.columns if c in valid_cols]
    # PKが欠けていたら異常
    for p in pk:
        if p not in cols:
            raise ValueError(f"COPY列に主キー {p} が含まれていません")

    df_use = df[cols].copy()

    # pandasのNAをNULLとして扱わせる
    csv_buf = io.StringIO()
    df_use.to_csv(csv_buf, index=False, na_rep="")
    csv_text = csv_buf.getvalue()

    tmp_name = f"tmp_{table.name}_{uuid4().hex[:8]}"

    # SQL文字列組み立て
    cols_q = ", ".join(q(c) for c in cols)
    pk_q   = ", ".join(q(p) for p in pk)
    upd_cols = [c for c in cols if c not in pk]
    set_clause = ", ".join(f"{q(c)}=EXCLUDED.{q(c)}" for c in upd_cols) if upd_cols else ""

    create_tmp_sql = f'CREATE TEMP TABLE {q(tmp_name)} (LIKE {q(table.name)} INCLUDING ALL);'
    copy_sql = f'COPY {q(tmp_name)} ({cols_q}) FROM STDIN WITH (FORMAT csv, HEADER true);'
    insert_sql = f'INSERT INTO {q(table.name)} ({cols_q}) SELECT {cols_q} FROM {q(tmp_name)} ' \
                 f'ON CONFLICT ({pk_q}) DO ' + ('NOTHING;' if not set_clause else f'UPDATE SET {set_clause};')
    drop_tmp_sql = f'DROP TABLE IF EXISTS {q(tmp_name)};'

    # 実行
    with eng.begin() as conn:
        # SQLAlchemy 2.0 互換のドライバ接続の取り出し
        driver_conn = getattr(conn.connection, "driver_connection", None)
        if driver_conn is None:
            driver_conn = conn.connection.connection  # fallback

        with driver_conn.cursor() as cur:
            cur.execute(create_tmp_sql)
            cur.copy_expert(copy_sql, io.StringIO(csv_text))
            cur.execute(insert_sql)
            cur.execute(drop_tmp_sql)

# ======================== 並列処理: ダウンロード & 正規化 ========================
def process_one_file(file_meta: dict) -> dict | None:
    """
    戻り値: { "table_name": str, "df": DataFrame, "store": str, "machine": str, "date": date, "file_id": str, "md5": str, "path": str }
    or {"error": "..."} / None（スキップ）
    """
    try:
        store, machine, date = parse_meta(file_meta["path"])
        if store not in COLUMN_MAP:
            return None

        drv = make_drive()  # スレッド毎に生成
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
        return {"error": f"{file_meta.get('path','(unknown)')} 処理エラー: {e}"}

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

    c3, c4 = st.columns(2)
    max_files = c3.slider("最大ファイル数（1回の実行上限）", 10, 2000, 300, step=10,
                          help="大量フォルダは分割して取り込み（タイムアウト回避）")
    workers = c4.slider("並列ダウンロード数", 1, 8, 4,
                        help="並列数が多すぎるとAPI制限に当たる可能性があります")

    use_copy = st.checkbox("DB書き込みをCOPYで高速化（推奨）", value=True,
                           help="一時テーブルにCOPY→まとめてUPSERT。失敗時は自動で通常UPSERTにフォールバックします。")

    if st.button("🚀 インポート実行", disabled=not folder_id):
        try:
            files_all = list_csv_recursive(folder_id)
            files = [
                f for f in files_all
                if imp_start <= parse_meta(f['path'])[2] <= imp_end
            ]
        except Exception as e:
            st.error(f"ファイル一覧取得エラー: {e}")
            st.stop()

        imported_md5 = get_imported_md5_map()
        targets, skipped = [], 0
        for f in files:
            fid = f["id"]; md5 = f.get("md5Checksum") or ""
            if fid in imported_md5 and imported_md5[fid] == md5:
                skipped += 1
                continue
            targets.append(f)

        if not targets:
            st.success("差分はありません（すべて最新）")
            st.stop()

        targets = targets[:max_files]

        st.write(f"🔍 対象 CSV: **{len(targets)} 件**（スキップ {skipped} 件）")
        bar = st.progress(0.0)
        status = st.empty()
        created_tables: dict[str, sa.Table] = {}
        import_log_entries = []
        errors = []

        # 結果をテーブル単位で集約（COPY一括に使う）
        bucket: dict[str, list[dict]] = defaultdict(list)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futures = {ex.submit(process_one_file, f): f for f in targets}
            done_count = 0
            for fut in as_completed(futures):
                done_count += 1
                res = fut.result()
                if res is None:
                    bar.progress(done_count / len(futures))
                    continue
                if "error" in res:
                    errors.append(res["error"])
                    bar.progress(done_count / len(futures))
                    continue

                table_name = res["table_name"]
                bucket[table_name].append(res)

                status.text(f"処理完了: {res['path']}")
                bar.progress(done_count / len(futures))

        # 書き込みフェーズ
        for table_name, items in bucket.items():
            # テーブル準備
            if table_name not in created_tables:
                tbl = ensure_store_table(items[0]["store"])
                created_tables[table_name] = tbl
            else:
                tbl = created_tables[table_name]

            # valid列に揃え＆結合
            valid_cols = [c.name for c in tbl.c]
            dfs = []
            for res in items:
                df = res["df"]
                # 足りない列は NULL で追加
                for c in valid_cols:
                    if c not in df.columns:
                        df[c] = pd.NA
                df = df[[c for c in df.columns if c in valid_cols]]
                dfs.append(df)

            df_all = pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame(columns=valid_cols)

            # COPY→MERGE または 通常UPSERT
            if use_copy:
                try:
                    bulk_upsert_copy_merge(tbl, df_all)
                except Exception as e:
                    errors.append(f"{table_name} COPY高速化失敗のため通常UPSERTで再試行: {e}")
                    # フォールバック（ファイル単位で安全に）
                    with eng.begin() as conn:
                        for res in items:
                            df_one = res["df"]
                            df_one = df_one[[c for c in df_one.columns if c in valid_cols]]
                            try:
                                upsert_dataframe(conn, tbl, df_one)
                            except Exception as ie:
                                errors.append(f"{res['path']} 通常UPSERTでも失敗: {ie}")
            else:
                with eng.begin() as conn:
                    upsert_dataframe(conn, tbl, df_all)

            # import_log 追加
            for res in items:
                import_log_entries.append({
                    "file_id": res["file_id"],
                    "md5": res["md5"],
                    "path": res["path"],
                    "store": res["store"],
                    "machine": res["machine"],
                    "date": res["date"],
                    "rows": int(len(res["df"])),
                })

        # import_log反映
        upsert_import_log(import_log_entries)

        status.text("")
        if errors:
            st.warning("一部でエラーが発生しました。詳細：")
            for msg in errors[:50]:
                st.write("- " + msg)
            if len(errors) > 50:
                st.write(f"... ほか {len(errors)-50} 件")
        st.success("インポート完了！")

# ========================= 可視化モード =========================
if mode == "📊 可視化":
    st.header("DB 可視化")
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

    with eng.connect() as conn:
        row = conn.execute(sa.text(f"SELECT MIN(date), MAX(date) FROM {table_name}")).first()
        min_date, max_date = (row or (None, None))

    if not (min_date and max_date):
        st.info("このテーブルには日付データがありません。まず取り込みを実行してください。")
        st.stop()

    c1, c2 = st.columns(2)
    vis_start = c1.date_input(
        "開始日", value=min_date, min_value=min_date, max_value=max_date, key=f"visual_start_{table_name}"
    )
    vis_end   = c2.date_input(
        "終了日", value=max_date, min_value=min_date, max_value=max_date, key=f"visual_end_{table_name}"
    )

    needed_cols = tuple(c.name for c in tbl.c)

    @st.cache_data
    def get_machines(table_name: str, start: dt.date, end: dt.date, _cols_key: tuple):
        t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
        q = sa.select(t.c.機種).where(t.c.date.between(start, end)).distinct()
        with eng.connect() as conn:
            return [r[0] for r in conn.execute(q)]

    machines = get_machines(table_name, vis_start, vis_end, needed_cols)
    if not machines:
        st.warning("指定期間にデータがありません")
        st.stop()

    machine_sel = st.selectbox("機種選択", machines)
    show_avg = st.checkbox("全台平均を表示")

    @st.cache_data
    def get_data(table_name: str, machine: str, start: dt.date, end: dt.date, _cols_key: tuple):
        t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
        q = sa.select(t).where(
            t.c.機種 == machine,
            t.c.date.between(start, end)
        ).order_by(t.c.date)
        return pd.read_sql(q, eng)

    df = get_data(table_name, machine_sel, vis_start, vis_end, needed_cols)
    if df.empty:
        st.warning("データがありません")
        st.stop()

    if show_avg:
        df_plot = (
            df.groupby("date", as_index=False)["合成確率"]
              .mean()
              .rename(columns={"合成確率": "plot_val"})
        )
        title = f"📈 全台平均 合成確率 | {machine_sel}"
    else:
        @st.cache_data
        def get_slots(table_name: str, machine: str, start: dt.date, end: dt.date, _cols_key: tuple):
            t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
            q = sa.select(t.c.台番号).where(
                t.c.機種 == machine, t.c.date.between(start, end)
            ).distinct().order_by(t.c.台番号)
            with eng.connect() as conn:
                vals = [r[0] for r in conn.execute(q) if r[0] is not None]
            return [int(v) for v in vals]

        slots = get_slots(table_name, machine_sel, vis_start, vis_end, needed_cols)
        if not slots:
            st.warning("台番号のデータが見つかりません")
            st.stop()
        slot_sel = st.selectbox("台番号", slots)
        df_plot = df[df["台番号"] == slot_sel].rename(columns={"合成確率": "plot_val"})
        title = f"📈 合成確率 | {machine_sel} | 台 {slot_sel}"

    thresholds = setting_map.get(machine_sel, {})
    df_rules = pd.DataFrame([{"setting": k, "value": v} for k, v in thresholds.items()]) if thresholds else pd.DataFrame(columns=["setting","value"])
    legend_sel = alt.selection_multi(fields=["setting"], bind="legend")

    y_axis = alt.Axis(
        title="合成確率",
        format=".4f",
        labelExpr="isValid(datum.value) ? (datum.value==0 ? '0' : '1/'+format(round(1/datum.value),'d')) : ''"
    )

    base = alt.Chart(df_plot).mark_line().encode(
        x="date:T",
        y=alt.Y("plot_val:Q", axis=y_axis),
        tooltip=[
            alt.Tooltip("date:T", title="日付"),
            alt.Tooltip("plot_val:Q", title="値", format=".4f")
        ],
    ).properties(height=400)

    if not df_rules.empty:
        rules = alt.Chart(df_rules).mark_rule(strokeDash=[4, 2]).encode(
            y="value:Q",
            color=alt.Color("setting:N", legend=alt.Legend(title="設定ライン")),
            opacity=alt.condition(legend_sel, alt.value(1), alt.value(0))
        ).add_selection(legend_sel)
        chart = base + rules
    else:
        chart = base

    st.subheader(title)
    st.altair_chart(chart, use_container_width=True)
