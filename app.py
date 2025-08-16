# app.py
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
@st.cache_data(show_spinner=False)
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
            val[(denom <= 0) | (~denom.notna())] = 0  # 0・負値・欠損は0
            df.loc[mask_div, col] = val

        # 数値直書き（>1 は 1/値, <=1 はそのまま）
        num = pd.to_numeric(ser[~mask_div], errors="coerce")
        conv = num.copy()
        conv[num > 1] = 1.0 / num[num > 1]
        df.loc[~mask_div, col] = conv.fillna(0)

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
@st.cache_data(show_spinner=False)
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

# ======================== テーブル作成（PK/Index） ========================
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
        # 代表的なインデックス
        with eng.begin() as conn:
            conn.execute(sa.text(f'CREATE INDEX IF NOT EXISTS idx_{safe}_kisyudate ON "{safe}" ("機種", "date");'))
            conn.execute(sa.text(f'CREATE INDEX IF NOT EXISTS idx_{safe}_date ON "{safe}" ("date");'))
            conn.execute(sa.text(f'CREATE INDEX IF NOT EXISTS idx_{safe}_slotdate ON "{safe}" ("機種","台番号","date");'))
        return t
    return sa.Table(safe, meta, autoload_with=eng)

# ======================== アップサート ========================
def upsert_dataframe(conn, table, df: pd.DataFrame, pk=("date", "機種", "台番号")):
    rows = df.to_dict(orient="records")
    if not rows:
        return
    stmt = pg_insert(table).values(rows)
    update_cols = {c.name: stmt.excluded[c.name] for c in table.c if c.name not in pk}
    stmt = stmt.on_conflict_do_update(index_elements=list(pk), set_=update_cols)
    conn.execute(stmt)

# ========================= 取り込みモード =========================
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

    preview_n = st.number_input("先頭プレビュー件数（検証用）", 0, 10, 3, help="読み込んだCSVの先頭数行を表示してマッピング確認")

    colx, coly, colz = st.columns(3)
    dry_run   = colx.checkbox("ドライラン（検証のみ）", value=False)
    delta_only = coly.checkbox("差分のみ（既存MAX日付より新しいCSV）", value=True)
    show_path = colz.checkbox("進捗にファイルパス表示", value=True)

    if st.button("🚀 インポート実行", disabled=not folder_id):
        try:
            all_files = list_csv_recursive(folder_id)
            files = []
            for f in all_files:
                store, machine, date = parse_meta(f["path"])
                if imp_start <= date <= imp_end:
                    files.append({**f, "store": store, "machine": machine, "date": date})
        except Exception as e:
            st.error(f"ファイル一覧取得エラー: {e}")
            st.stop()

        st.write(f"🔍 対象 CSV: **{len(files)} 件**")
        bar = st.progress(0.0)
        current_file = st.empty()

        # 差分用に既存MAX(date)を取得
        latest_by_store = {}
        if delta_only and files:
            with eng.connect() as conn:
                for store in set([f["store"] for f in files]):
                    tname = "slot_" + store.replace(" ", "_")
                    try:
                        row = conn.execute(sa.text(f'SELECT MAX(date) FROM "{tname}"')).first()
                        latest_by_store[tname] = row[0] if row else None
                    except Exception:
                        latest_by_store[tname] = None

        created_tables = {}
        for i, f in enumerate(sorted(files, key=lambda x: (x["store"], x["machine"], x["date"])), 1):
            if show_path:
                current_file.text(f"処理中ファイル: {f['path']}")
            try:
                raw = drive.files().get_media(fileId=f["id"]).execute()
                store, machine, date = f["store"], f["machine"], f["date"]
                table_name = "slot_" + store.replace(" ", "_")

                # 差分スキップ
                if delta_only and latest_by_store.get(table_name) and date <= latest_by_store[table_name]:
                    bar.progress(i / len(files))
                    continue

                # テーブル確保
                if table_name not in created_tables:
                    tbl = ensure_store_table(store)
                    created_tables[table_name] = tbl
                else:
                    tbl = created_tables[table_name]

                # 読み・正規化
                df = load_and_normalize(raw, store)
                if df.empty:
                    bar.progress(i / len(files)); continue

                # 先頭プレビュー
                if preview_n > 0 and i <= preview_n:
                    st.caption(f"📄 プレビュー: {f['path']}")
                    st.dataframe(df.head(min(5, len(df))))

                df["機種"], df["date"] = machine, date

                # テーブルに存在する列のみ
                valid_cols = [c.name for c in tbl.c]
                df = df[[c for c in df.columns if c in valid_cols]]

                if not dry_run:
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

    # URLクエリから初期値を復元
    qp = st.query_params
    qp_table   = qp.get("table", [""])[0] if isinstance(qp.get("table"), list) else qp.get("table", "")
    qp_machine = qp.get("machine", [""])[0] if isinstance(qp.get("machine"), list) else qp.get("machine", "")
    qp_start   = qp.get("start", [""])[0] if isinstance(qp.get("start"), list) else qp.get("start", "")
    qp_end     = qp.get("end", [""])[0] if isinstance(qp.get("end"), list) else qp.get("end", "")
    qp_avg     = qp.get("avg", ["false"])[0] if isinstance(qp.get("avg"), list) else qp.get("avg", "false")

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

    table_name = st.selectbox("テーブル選択", tables, index=max(0, tables.index(qp_table)) if qp_table in tables else 0)
    if not table_name:
        st.error("テーブルが選択されていません")
        st.stop()

    tbl = sa.Table(table_name, sa.MetaData(), autoload_with=eng)

    # 最小/最大日付
    with eng.connect() as conn:
        row = conn.execute(sa.text(f'SELECT MIN(date), MAX(date) FROM "{table_name}"')).first()
        min_date, max_date = (row or (None, None))

    if not (min_date and max_date):
        st.info("このテーブルには日付データがありません。まず取り込みを実行してください。")
        st.stop()

    # プリセットボタン
    def apply_preset(preset: str):
        today = dt.date.today()
        if preset == "today":
            return today, today
        if preset == "this_week":
            start = today - dt.timedelta(days=today.weekday())
            return start, today
        if preset == "this_month":
            start = today.replace(day=1)
            return start, today
        return None

    c0, c1, c2, c3, c4, c5 = st.columns(6)
    if c0.button("📅 今日"):
        s,e = apply_preset("today"); st.session_state[f"visual_start_{table_name}"]=s; st.session_state[f"visual_end_{table_name}"]=e
    if c1.button("📅 今週"):
        s,e = apply_preset("this_week"); st.session_state[f"visual_start_{table_name}"]=s; st.session_state[f"visual_end_{table_name}"]=e
    if c2.button("📅 今月"):
        s,e = apply_preset("this_month"); st.session_state[f"visual_start_{table_name}"]=s; st.session_state[f"visual_end_{table_name}"]=e

    c1a, c2a = st.columns(2)
    vis_start = c1a.date_input(
        "開始日",
        value=dt.date.fromisoformat(qp_start) if qp_start else min_date,
        min_value=min_date, max_value=max_date,
        key=f"visual_start_{table_name}"
    )
    vis_end   = c2a.date_input(
        "終了日",
        value=dt.date.fromisoformat(qp_end) if qp_end else max_date,
        min_value=min_date, max_value=max_date,
        key=f"visual_end_{table_name}"
    )

    # カラムキー安定化
    needed_cols = tuple(c.name for c in tbl.c)

    @st.cache_data(show_spinner=False)
    def get_machines(table_name: str, start: dt.date, end: dt.date, _cols_key: tuple):
        t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
        q = sa.select(t.c.機種).where(t.c.date.between(start, end)).distinct()
        with eng.connect() as conn:
            return [r[0] for r in conn.execute(q)]

    machines = get_machines(table_name, vis_start, vis_end, needed_cols)
    if not machines:
        st.warning("指定期間にデータがありません")
        st.stop()

    default_machine_index = max(0, machines.index(qp_machine)) if qp_machine in machines else 0
    machine_sel = st.selectbox(f"機種選択（{len(machines)}）", machines, index=default_machine_index)

    c_filter1, c_filter2, c_filter3 = st.columns(3)
    only_5       = c_filter1.checkbox("5のつく日だけ")
    only_7       = c_filter2.checkbox("7のつく日だけ")
    only_weekend = c_filter3.checkbox("土日だけ")

    show_avg = st.checkbox("全台平均を表示", value=(qp_avg == "true"))

    @st.cache_data(show_spinner=False)
    def get_data(table_name: str, machine: str, start: dt.date, end: dt.date, _cols_key: tuple):
        t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
        q = sa.select(t).where(
            t.c.機種 == machine,
            t.c.date.between(start, end)
        ).order_by(t.c.date)
        df = pd.read_sql(q, eng)
        # 型調整
        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"]).dt.date
        if "台番号" in df.columns:
            # 整数表示（欠損はNAのまま）
            try:
                df["台番号"] = pd.to_numeric(df["台番号"], errors="coerce").astype("Int64")
            except Exception:
                pass
        return df

    df = get_data(table_name, machine_sel, vis_start, vis_end, needed_cols)
    if df.empty:
        st.warning("データがありません")
        st.stop()

    # 日付フィルタ（5/7/土日）
    srs_date = pd.to_datetime(df["date"])
    if only_5:
        df = df[(srs_date.dt.day % 10) == 5]
    if only_7:
        df = df[(srs_date.dt.day % 10) == 7]
    if only_weekend:
        df = df[srs_date.dt.weekday.isin([5, 6])]
    if df.empty:
        st.warning("フィルタ条件でデータがなくなりました")
        st.stop()

    # 表示モード（1/◯ or 実数）
    display_mode = st.radio("表示モード", ("1/◯（直感）", "実数（0〜1）"), horizontal=True)

    # グラフ用データ
    if show_avg:
        df_plot = (
            df.groupby("date", as_index=False)["合成確率"]
              .mean()
              .rename(columns={"合成確率": "plot_val"})
        )
        title = f"📈 全台平均 合成確率 | {machine_sel}"
        # 台番号選択UIは非表示
        slot_sel = None
    else:
        @st.cache_data(show_spinner=False)
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

    # 移動平均
    win = st.slider("移動平均ウィンドウ（日）", 1, 14, 1, help="1なら現状維持、5や7で滑らかに")
    df_plot_ma = df_plot.copy()
    if win > 1:
        df_plot_ma["ma"] = df_plot_ma["plot_val"].rolling(win, min_periods=1).mean()
    else:
        df_plot_ma["ma"] = df_plot_ma["plot_val"]

    # 設定ライン
    thresholds = setting_map.get(machine_sel, {})
    df_rules = pd.DataFrame([{"setting": k, "value": v} for k, v in thresholds.items()]) if thresholds else pd.DataFrame(columns=["setting","value"])

    # Y軸
    if display_mode == "1/◯（直感）":
        y_axis = alt.Axis(
            title="合成確率",
            format=".4f",
            labelExpr="isValid(datum.value) ? (datum.value==0 ? '0' : '1/'+format(round(1/datum.value),'d')) : ''"
        )
    else:
        y_axis = alt.Axis(title="合成確率（実数）", format=".4f")

    base = alt.Chart(df_plot_ma).mark_line().encode(
        x="date:T",
        y=alt.Y("plot_val:Q", axis=y_axis),
        tooltip=[
            alt.Tooltip("date:T", title="日付"),
            alt.Tooltip("plot_val:Q", title="値", format=".4f"),
            alt.Tooltip("ma:Q", title="移動平均", format=".4f")
        ],
    ).properties(height=400)

    ma_line = alt.Chart(df_plot_ma).mark_line(strokeDash=[4,2]).encode(
        x="date:T", y="ma:Q", color=alt.value("gray")
    )

    chart = base + ma_line

    if not df_rules.empty:
        legend_sel = alt.selection_multi(fields=["setting"], bind="legend")
        rules = alt.Chart(df_rules).mark_rule(strokeDash=[4, 2]).encode(
            y="value:Q",
            color=alt.Color("setting:N", legend=alt.Legend(title="設定ライン")),
            opacity=alt.condition(legend_sel, alt.value(1), alt.value(0.2))
        ).add_selection(legend_sel)
        chart = chart + rules

    st.subheader(title)
    st.altair_chart(chart, use_container_width=True)

    # 表とダウンロード
    st.caption("📋 データ（期間・機種・台の現在ビュー）")
    df_show = df.sort_values(["date","台番号"], na_position="last")
    st.dataframe(df_show)
    csv = df_show.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "⬇️ 表データ（CSV）",
        data=csv,
        file_name=f"{table_name}_{machine_sel}_{vis_start}_{vis_end}{'' if slot_sel is None else f'_slot{slot_sel}'}.csv",
        mime="text/csv"
    )

    # PNG保存（環境により不可の場合あり）
    try:
        from altair_saver import save
        png_path = f"{table_name}_{machine_sel}_{vis_start}_{vis_end}{'' if slot_sel is None else f'_slot{slot_sel}'}.png"
        save(chart, png_path)
        with open(png_path, "rb") as f:
            st.download_button("🖼 グラフPNGをダウンロード", data=f, file_name=png_path, mime="image/png")
    except Exception:
        st.info("PNG保存はこの環境では無効のため、CSVのみ提供中です。")

    # ヒートマップ（機種全体）
    st.subheader("🗺 台番号×日付ヒートマップ（合成確率）")
    df_heat = df.copy()
    if "台番号" in df_heat.columns:
        df_heat = df_heat.dropna(subset=["台番号", "合成確率"])
        try:
            df_heat["台番号"] = pd.to_numeric(df_heat["台番号"], errors="coerce").astype("Int64")
        except Exception:
            pass
        df_heat["inv"] = df_heat["合成確率"].replace(0, pd.NA)
        df_heat["inv"] = df_heat["inv"].apply(lambda x: None if pd.isna(x) else 1.0/x)
        heat = alt.Chart(df_heat).mark_rect().encode(
            x=alt.X("date:T", title="日付"),
            y=alt.Y("台番号:O", sort="ascending"),
            color=alt.Color("inv:Q", title="1/合成確率（大きい=熱い）"),
            tooltip=[
                alt.Tooltip("date:T", title="日付"),
                alt.Tooltip("台番号:O", title="台番号"),
                alt.Tooltip("合成確率:Q", format=".4f"),
                alt.Tooltip("inv:Q", title="1/合成", format=".0f"),
            ]
        ).properties(height=300)
        st.altair_chart(heat, use_container_width=True)
    else:
        st.info("台番号列が無いためヒートマップは表示しません。")

    # お気に入り（簡易）
    st.divider()
    st.caption("⭐ よく使う組合せを保存")
    if "favorites" not in st.session_state:
        st.session_state["favorites"] = []
    fav_name = st.text_input("お気に入り名（例: 武蔵境_マイジャグV_台237）")
    if st.button("⭐ 追加", disabled=not fav_name):
        st.session_state["favorites"].append({
            "name": fav_name,
            "table": table_name,
            "machine": machine_sel,
            "start": vis_start.isoformat(),
            "end": vis_end.isoformat(),
            "avg": str(show_avg).lower()
        })
        st.success("お気に入りに追加しました")

    if st.session_state["favorites"]:
        colf1, colf2 = st.columns([3,1])
        sel_fav = colf1.selectbox("お気に入りを呼び出し", [f['name'] for f in st.session_state["favorites"]])
        if colf2.button("呼び出し"):
            fav = next(f for f in st.session_state["favorites"] if f["name"]==sel_fav)
            st.query_params.update({
                "table": fav["table"],
                "machine": fav["machine"],
                "start": fav["start"],
                "end": fav["end"],
                "avg": fav["avg"]
            })
            st.experimental_rerun()

    # URLクエリへ現在状態を反映（共有用）
    st.query_params.update({
        "table": table_name,
        "machine": machine_sel,
        "start": vis_start.isoformat(),
        "end": vis_end.isoformat(),
        "avg": str(show_avg).lower()
    })
