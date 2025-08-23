あなた:
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
# 「最大持ち玉」と「最大持玉」の表記ゆれを両方吸収
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
                fields="nextPageToken, files(id,name,mimeType)",
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
            # 0, 負値, 欠損は0に
            val[(denom <= 0) | (~denom.notna())] = 0
            df.loc[mask_div, col] = val

        # 数値直書き（>1 は 1/値, <=1 はそのまま）
        num = pd.to_numeric(ser[ ~mask_div ], errors="coerce")
        conv = num.copy()
        conv[num > 1] = 1.0 / num[num > 1]
        conv = conv.fillna(0)
        df.loc[~mask_div, col] = conv

        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0).astype(float)

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
@st.cache_data
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
        # 重複を除いた正規化後の列名
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

# ======================== アップサート（重複耐性） ========================
def upsert_dataframe(conn, table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
    rows = df.to_dict(orient="records")
    if not rows:
        return
    stmt = pg_insert(table).values(rows)
    update_cols = {c.name: stmt.excluded[c.name] for c in table.c if c.name not in pk}
    stmt = stmt.on_conflict_do_update(index_elements=list(pk), set_=update_cols)
    conn.execute(stmt)

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
                if imp_start <= parse_meta(f['path'])[2] <= imp_end
            ]
        except Exception as e:
            st.error(f"ファイル一覧取得エラー: {e}")
            st.stop()

        st.write(f"🔍 対象 CSV: **{len(files)} 件**")
        bar = st.progress(0.0)
        current_file = st.empty()
        created_tables = {}

        for i, f in enumerate(files, 1):
            current_file.text(f"処理中ファイル: {f['path']}")
            try:
                raw = drive.files().get_media(fileId=f["id"]).execute()
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
                    upsert_dataframe(conn, tbl, df)

            except Exception as e:
                st.error(f"{f['path']} 処理エラー: {e}")

            bar.progress(i / len(files))

        current_file.text("")
        st.success("インポート完了！")

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

    c1, c2 = st.columns(2)
    vis_start = c1.date_input(
        "開始日", value=min_date, min_value=min_date, max_value=max_date, key=f"visual_start_{table_name}"
    )
    vis_end   = c2.date_input(
        "終了日", value=max_date, min_value=min_date, max_value=max_date, key=f"visual_end_{table_name}"
    )

    # キャッシュキーを安定化するために、テーブル名と必要カラム名を渡す
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
            # Int64やfloat混在を避けて整数表示
            return [int(v) for v in vals]

        slots = get_slots(table_name, machine_sel, vis_start, vis_end, needed_cols)
        if not slots:
            st.warning("台番号のデータが見つかりません")
            st.stop()
        slot_sel = st.selectbox("台番号", slots)
        df_plot = df[df["台番号"] == slot_sel].rename(columns={"合成確率": "plot_val"})
        title = f"📈 合成確率 | {machine_sel} | 台 {slot_sel}"

    # 設定ライン
    thresholds = setting_map.get(machine_sel, {})
    df_rules = pd.DataFrame([{"setting": k, "value": v} for k, v in thresholds.items()]) if thresholds else pd.DataFrame(columns=["setting","value"])

    legend_sel = alt.selection_multi(fields=["setting"], bind="legend")

    # 0は0、>0は 1/x 表示（安全に）
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
