import io
import datetime as dt
import pandas as pd
import streamlit as st
import sqlalchemy as sa
from sqlalchemy.dialects.postgresql import insert as pg_insert
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
import altair as alt
import json

# -------- 設定ファイル読み込み --------
with open("setting.json", encoding="utf-8") as f:
    setting_map = json.load(f)

# -------- Streamlit UI 設定 --------
st.set_page_config(page_title="Slot Manager", layout="wide")
mode = st.sidebar.radio("モード", ("📥 データ取り込み", "📊 可視化"))
st.title("🎰 Slot Data Manager & Visualizer")

SA_INFO = st.secrets["gcp_service_account"]
PG_CFG  = st.secrets["connections"]["slot_db"]

# -------- Drive, DB 接続 --------
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

# -------- テーブルキャッシュ --------
@st.cache_resource
def get_table(table_name: str) -> sa.Table:
    try:
        meta = sa.MetaData()
        return sa.Table(table_name, meta, autoload_with=eng)
    except Exception as e:
        st.error(f"テーブル取得エラー ({table_name}): {e}")
        raise

# -------- 定義マッピング --------
COLUMN_MAP = {
    "メッセ武蔵境": {
        "台番号":           "台番号",
        "スタート回数":     "スタート回数",
        "累計スタート":     "累計スタート",
        "BB回数":          "BB回数",
        "RB回数":          "RB回数",
        "ART回数":         "ART回数",
        "最大持ち玉":       "最大持玉",
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

# -------- ファイル列挙 --------
@st.cache_data
def list_csv_recursive(folder_id: str):
    all_files, queue = [], [(folder_id, "")]
    while queue:
        fid, cur = queue.pop()
        res = drive.files().list(
            q=f"'{fid}' in parents and trashed=false",
            fields="files(id,name,mimeType)",
            pageSize=1000
        ).execute()
        for f in res.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                queue.append((f["id"], f"{cur}/{f['name']}"))
            elif f["name"].lower().endswith(".csv"):
                all_files.append({**f, "path": f"{cur}/{f['name']}"})
    return all_files

# -------- 正規化 --------
def normalize(df_raw: pd.DataFrame, store: str) -> pd.DataFrame:
    df = df_raw.rename(columns=COLUMN_MAP[store])
    prob_cols = ["BB確率","RB確率","ART確率","合成確率"]
    for col in prob_cols:
        if col not in df.columns:
            continue
        ser = df[col].astype(str)
        mask_div = ser.str.contains("/")
        if mask_div.any():
            denom = ser[mask_div].str.split("/", expand=True)[1].astype(float)
            df.loc[mask_div, col] = denom.where(denom!=0, pd.NA).rdiv(1.0).fillna(0)
        num = pd.to_numeric(ser[~mask_div], errors="coerce")
        mask_gt1 = num > 1
        num.loc[mask_gt1] = 1.0 / num.loc[mask_gt1]
        df.loc[~mask_div, col] = num
        df[col] = df[col].astype(float)
    int_cols = [
        "台番号","累計スタート","スタート回数","BB回数",
        "RB回数","ART回数","最大持玉","最大差玉","前日最終スタート"
    ]
    for col in int_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")
    return df

# -------- 読み込み＋正規化 --------
@st.cache_data
def load_and_normalize(raw_bytes: bytes, store: str) -> pd.DataFrame:
    header = pd.read_csv(io.BytesIO(raw_bytes), encoding="shift_jis", nrows=0).columns.tolist()
    mapping_keys = list(dict.fromkeys(COLUMN_MAP[store].keys()))
    usecols = [col for col in mapping_keys if col in header]
    df_raw = pd.read_csv(
        io.BytesIO(raw_bytes), encoding="shift_jis", usecols=usecols,
        on_bad_lines="skip", engine="c"
    )
    return normalize(df_raw, store)

# -------- メタ情報解析 --------
def parse_meta(path: str):
    parts = path.strip("/").split("/")
    store, machine = parts[-3], parts[-2]
    date = dt.date.fromisoformat(parts[-1][-14:-4])
    return store, machine, date

# -------- テーブル作成 --------
def ensure_store_table(store: str):
    safe = "slot_" + store.replace(" ", "_")
    meta = sa.MetaData()
    if not eng.dialect.has_table(eng.connect(), safe):
        cols = [
            sa.Column("date", sa.Date),
            sa.Column("機種", sa.Text),
        ]
        unique_cols = list(dict.fromkeys(COLUMN_MAP[store].values()))
        for col_name in unique_cols:
            cols.append(sa.Column(col_name, sa.Double, nullable=True))
        cols.append(sa.PrimaryKeyConstraint("date", "機種", "台番号"))
        sa.Table(safe, meta, *cols)
        meta.create_all(eng)
    return sa.Table(safe, meta, autoload_with=eng)

# ========================= データ取り込み =========================
if mode == "📥 データ取り込み":
    st.header("Google Drive → Postgres インポート")
    folder_options = {
        "🧪 テスト用": "1MRQFPBahlSwdwhrqqBzudXL18y8-qOb8",
        "🚀 本番用":     "1hX8GQRuDm_E1A1Cu_fZudXL18y8-qOb8"
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
        created_tables = set()
        for i, f in enumerate(files, 1):
            current_file.text(f"処理中ファイル: {f['path']}")
            try:
                raw = drive.files().get_media(fileId=f["id"]).execute()
                store, machine, date = parse_meta(f["path"])
                table_name = "slot_" + store.replace(" ", "_")
                if table_name not in created_tables:
                    tbl = ensure_store_table(store)
                    created_tables.add(table_name)
                else:
                    tbl = get_table(table_name)
                df = load_and_normalize(raw, store)
                if df.empty:
                    continue
                df['機種'], df['date'] = machine, date
                df = df[[c for c in df.columns if c in tbl.c.keys()]]
                df.to_sql(name=tbl.name, con=eng, if_exists="append",
                          index=False, method="multi", chunksize=500)
            except Exception as e:
                st.error(f"{f['path']} 処理エラー: {e}")
            bar.progress(i / len(files))
        current_file.text("")
        st.success("インポート完了！")

# ========================= 可視化モード =========================
if mode == "📊 可視化":
    st.header("DB 可視化")
    try:
        tables = [r[0] for r in eng.connect().execute(sa.text(
            "SELECT tablename FROM pg_tables WHERE tablename LIKE 'slot_%'"
        ))]
    except Exception as e:
        st.error(f"テーブル一覧取得エラー: {e}")
        st.stop()
    table_name = st.selectbox("テーブル選択", tables)
    if table_name is None:
        st.error("テーブルが選択されていません")
        st.stop()
    tbl = get_table(table_name)

    # テーブル内の最小／最大日付を取得して date_input に設定
    with eng.connect() as conn:
        min_date, max_date = conn.execute(
            sa.text(f"SELECT MIN(date), MAX(date) FROM {table_name}")
        ).first()
    c1, c2 = st.columns(2)
    vis_start = c1.date_input(
        "開始日", value=min_date, min_value=min_date, max_value=max_date, key="visual_start_date"
    )
    vis_end   = c2.date_input(
        "終了日", value=max_date, min_value=min_date, max_value=max_date, key="visual_end_date"
    )

    @st.cache_data
    def get_machines(table_name: str, start: dt.date, end: dt.date):
        q = sa.select(tbl.c.機種).where(tbl.c.date.between(start, end)).distinct()
        return [r[0] for r in eng.connect().execute(q)]
    machines = get_machines(table_name, vis_start, vis_end)
    machine_sel = st.selectbox("機種選択", machines)
    show_avg = st.checkbox("全台平均を表示")

    @st.cache_data
    def get_data(table_name: str, machine: str, start: dt.date, end: dt.date):
        q = sa.select(tbl).where(tbl.c.機種 == machine,
                                    tbl.c.date.between(start, end)
                                ).order_by(tbl.c.date)
        return pd.read_sql(q, eng)
    df = get_data(table_name, machine_sel, vis_start, vis_end)
    if df.empty:
        st.warning("データがありません")
        st.stop()

    if show_avg:
        df_plot = df.groupby("date")["合成確率"].mean().reset_index().rename(columns={"合成確率": "plot_val"})
        title = f"📈 全台平均 合成確率 | {machine_sel}"
    else:
        @st.cache_data
        def get_slots(table_name: str, machine: str, start: dt.date, end: dt.date):
            q = sa.select(tbl.c.台番号).where(tbl.c.機種 == machine,
                                                tbl.c.date.between(start, end)
                                            ).distinct().order_by(tbl.c.台番号)
            return [int(r[0]) for r in eng.connect().execute(q) if r[0] is not None]
        slots = get_slots(table_name, machine_sel, vis_start, vis_end)
        slot_sel = st.selectbox("台番号", slots)
        df_plot = df[df["台番号"] == slot_sel].rename(columns={"合成確率": "plot_val"})
        title = f"📈 合成確率 | {machine_sel} | 台 {slot_sel}"

    thresholds = setting_map.get(machine_sel, {})
    df_rules = pd.DataFrame([{"setting": k, "value": v} for k, v in thresholds.items()])
    legend_sel = alt.selection_multi(fields=["setting"], bind="legend")
    y_axis = alt.Axis(title="合成確率", format=".4f",
                      labelExpr=("datum.value==0?'0':'1/'+format(round(1/datum.value),'d')"))
    base = alt.Chart(df_plot).mark_line().encode(
        x="date:T",
        y=alt.Y("plot_val:Q", axis=y_axis),
        tooltip=["date", alt.Tooltip("plot_val:Q", title="値", format=".4f")]
    ).properties(height=400)
    rules = alt.Chart(df_rules).mark_rule(strokeDash=[4,2]).encode(
        y="value:Q",
        color=alt.Color("setting:N", legend=alt.Legend(title="設定ライン")),
        opacity=alt.condition(legend_sel, alt.value(1), alt.value(0))
    ).add_selection(legend_sel)

    st.subheader(title)
    st.altair_chart(base + rules, use_container_width=True)
