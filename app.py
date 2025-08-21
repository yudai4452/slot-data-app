import io
import re
import unicodedata
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

# ======================== ユーティリティ（キー/表記ゆれ正規化） ========================
def norm_key(s: str) -> str:
    s = unicodedata.normalize("NFKC", s or "")
    s = s.replace("ー", "-").replace("　", " ").strip()
    return s

# 店舗名の正規化版マップ（キーを正規化）
COLUMN_MAP_RAW = {
    "メッセ武蔵境": {
        "台番号": "台番号", "スタート回数": "スタート回数", "累計スタート": "累計スタート",
        "BB回数": "BB回数", "RB回数": "RB回数", "ART回数": "ART回数",
        "最大持ち玉": "最大持玉", "最大持玉": "最大持玉",
        "BB確率": "BB確率", "RB確率": "RB確率", "ART確率": "ART確率",
        "合成確率": "合成確率", "前日最終スタート": "前日最終スタート",
    },
    "ジャンジャンマールゴット分倍河原": {
        "台番号": "台番号", "累計スタート": "累計スタート", "BB回数": "BB回数", "RB回数": "RB回数",
        "最大持ち玉": "最大持玉", "最大持玉": "最大持玉",
        "BB確率": "BB確率", "RB確率": "RB確率", "合成確率": "合成確率",
        "前日最終スタート": "前日最終スタート", "スタート回数": "スタート回数",
    },
    "プレゴ立川": {
        "台番号": "台番号", "累計スタート": "累計スタート", "BB回数": "BB回数", "RB回数": "RB回数",
        "最大差玉": "最大差玉",
        "BB確率": "BB確率", "RB確率": "RB確率", "合成確率": "合成確率",
        "前日最終スタート": "前日最終スタート", "スタート回数": "スタート回数",
    },
}
COLUMN_MAP = {norm_key(k): v for k, v in COLUMN_MAP_RAW.items()}

# 一部カラムの表記ゆれ別名（あれば採用）
FALLBACK_ALIASES = {
    "最大持玉": ["最大持ち玉"],
    "最大差玉": ["最大差枚"],
}

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
        return sa.create_engine(url, pool_pre_ping=True, echo=False)
    except Exception as e:
        st.error(f"DB接続エラー: {e}")
        return None

eng = engine()
if eng is None:
    st.stop()

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
    # 処理順の安定化
    all_files.sort(key=lambda x: x["path"])
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
def _build_usecols(header: list[str], store_norm: str) -> list[str]:
    mapping = COLUMN_MAP[store_norm].copy()
    # エイリアス適用：ヘッダに存在する別名を正規名へ吸収
    for canon, aliases in FALLBACK_ALIASES.items():
        for a in aliases:
            if a in header and canon not in header and a in mapping:
                mapping[canon] = mapping[a]
    # mapping キーのうちヘッダにあるものだけ採用
    keys = list(dict.fromkeys(k for k in mapping.keys() if k in header))
    return keys

def normalize(df_raw: pd.DataFrame, store_norm: str) -> pd.DataFrame:
    df = df_raw.rename(columns=COLUMN_MAP[store_norm])

    # 確率列を実数(0〜1)へ揃える
    prob_cols = ["BB確率", "RB確率", "ART確率", "合成確率"]
    for col in prob_cols:
        if col not in df.columns:
            continue
        ser = df[col].astype(str)
        mask_div = ser.str.contains("/", na=False)

        # "1/x" 形式
        if mask_div.any():
            denom = pd.to_numeric(ser[mask_div].str.split("/", expand=True)[1], errors="coerce")
            val = 1.0 / denom
            val[(denom <= 0) | (~denom.notna())] = 0
            df.loc[mask_div, col] = val

        # 数値直書き（>1 は 1/値, <=1 はそのまま）
        num = pd.to_numeric(ser[~mask_div], errors="coerce")
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
def load_and_normalize(raw_bytes: bytes, store_raw: str) -> pd.DataFrame:
    store_norm = norm_key(store_raw)
    if store_norm not in COLUMN_MAP:
        raise ValueError(f"未対応の店舗名です: {store_raw}")
    # ヘッダ確認
    header = pd.read_csv(io.BytesIO(raw_bytes), encoding="shift_jis", nrows=0).columns.tolist()
    usecols = _build_usecols(header, store_norm)
    df_raw = pd.read_csv(
        io.BytesIO(raw_bytes),
        encoding="shift_jis",
        usecols=usecols,
        on_bad_lines="skip",
        engine="c",
    )
    return normalize(df_raw, store_norm)

# ======================== テーブル作成 ========================
def ensure_store_table(store_raw: str):
    safe = "slot_" + norm_key(store_raw).replace(" ", "_")
    insp = inspect(eng)
    meta = sa.MetaData()
    if not insp.has_table(safe):
        cols = [
            sa.Column("date", sa.Date, nullable=False),
            sa.Column("機種", sa.Text, nullable=False),
            sa.Column("台番号", sa.Integer, nullable=False),
        ]
        unique_cols = list(dict.fromkeys(COLUMN_MAP[norm_key(store_raw)].values()))
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
        # 推奨インデックス
        with eng.begin() as conn:
            conn.execute(sa.text(f"CREATE INDEX IF NOT EXISTS idx_{safe}_kisyudate ON {safe}(機種, date)"))
            conn.execute(sa.text(f"CREATE INDEX IF NOT EXISTS idx_{safe}_date ON {safe}(date)"))
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

# ========================= データ取り込み =========================
if mode == "📥 データ取り込み":
    st.header("Google Drive → Postgres インポート")
    folder_options = {
        "🧪 テスト用": "1MRQFPBahlSwdwhrqqBzudXL18y8-qOb8",
        "🚀 本番用":   "1hX8GQRuDm_E1A1Cu_fXorvwxv-XF7Ynl",
    }
    sel_label = st.selectbox("フォルダタイプ", list(folder_options.keys()))
    with st.expander("高度なオプション（通常は不要）", expanded=False):
        folder_id = st.text_input("Google Drive フォルダ ID を手入力", value=folder_options[sel_label])
        dry_run = st.checkbox("ドライラン（DBには書き込まない）", value=False)
        exclude_kw = st.text_input("ファイル名に含まれていたら除外（カンマ区切り）", value="サンプル,テスト").strip()
    if not folder_id:
        folder_id = folder_options[sel_label]

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
        exclude_list = [x.strip() for x in exclude_kw.split(",") if x.strip()]

        for i, f in enumerate(files, 1):
            # 除外ルール
            if any(x in f["name"] for x in exclude_list):
                bar.progress(i / len(files)); continue

            current_file.text(f"処理中ファイル: {f['path']}")
            try:
                raw = drive.files().get_media(fileId=f["id"]).execute()
                store, machine, date = parse_meta(f["path"])
                table_name = "slot_" + norm_key(store).replace(" ", "_")
                if table_name not in created_tables:
                    tbl = ensure_store_table(store)
                    created_tables[table_name] = tbl
                else:
                    tbl = created_tables[table_name]

                df = load_and_normalize(raw, store)
                if df.empty:
                    bar.progress(i / len(files)); continue

                df["機種"], df["date"] = machine, date
                valid_cols = [c.name for c in tbl.c]
                df = df[[c for c in df.columns if c in valid_cols]]

                if not dry_run:
                    with eng.begin() as conn:
                        upsert_dataframe(conn, tbl, df)

            except Exception as e:
                st.error(f"{f['path']} 処理エラー: {e}")

            bar.progress(i / len(files))

        current_file.text("")
        st.success("インポート完了！" + ("（ドライラン）" if dry_run else ""))

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

    # URLパラメータから初期化
    qparams = st.query_params
    default_table = qparams.get("table") if "table" in qparams else None
    if default_table in tables:
        table_name = st.selectbox("テーブル選択", tables, index=tables.index(default_table))
    else:
        table_name = st.selectbox("テーブル選択", tables)

    tbl = sa.Table(table_name, sa.MetaData(), autoload_with=eng)

    # 最小/最大日付
    with eng.connect() as conn:
        row = conn.execute(sa.text(f"SELECT MIN(date), MAX(date) FROM {table_name}")).first()
        min_date, max_date = (row or (None, None))

    if not (min_date and max_date):
        st.info("このテーブルには日付データがありません。まず取り込みを実行してください。")
        st.stop()

    # 期間プリセット
    preset = st.radio(
        "期間プリセット",
        ["過去7日", "過去30日", "全期間", "カスタム"],
        horizontal=True,
        index=1,
        help="よく使う期間をワンタップで切替"
    )

    if preset == "過去7日":
        vis_start, vis_end = max_date - dt.timedelta(days=6), max_date
    elif preset == "過去30日":
        vis_start, vis_end = max_date - dt.timedelta(days=29), max_date
    elif preset == "全期間":
        vis_start, vis_end = min_date, max_date
    else:
        c1, c2 = st.columns(2)
        vis_start = c1.date_input(
            "開始日", value=min_date, min_value=min_date, max_value=max_date, key=f"visual_start_{table_name}"
        )
        vis_end   = c2.date_input(
            "終了日", value=max_date, min_value=min_date, max_value=max_date, key=f"visual_end_{table_name}"
        )

    needed_cols = tuple(c.name for c in tbl.c)

    # 機種（人気順）
    @st.cache_data
    def get_machines_with_freq(table_name: str, start: dt.date, end: dt.date, _cols_key: tuple):
        t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
        q = sa.select(t.c.機種, sa.func.count().label("n")).where(
            t.c.date.between(start, end)
        ).group_by(t.c.機種).order_by(sa.desc("n"), t.c.機種)
        with eng.connect() as conn:
            return [r[0] for r in conn.execute(q)]

    machines = get_machines_with_freq(table_name, vis_start, vis_end, needed_cols)
    if not machines:
        st.warning("指定期間にデータがありません"); st.stop()

    # 機種検索（前方一致→部分一致）
    q_machine = st.text_input("機種名で検索", placeholder="例: マイジャグラー")
    if q_machine:
        filtered_machines = [m for m in machines if m.startswith(q_machine)] or \
                            [m for m in machines if q_machine in m]
    else:
        filtered_machines = machines

    default_machine = qparams.get("machine") if "machine" in qparams else None
    if default_machine in filtered_machines:
        machine_sel = st.selectbox("機種選択", filtered_machines, index=filtered_machines.index(default_machine))
    else:
        machine_sel = st.selectbox("機種選択", filtered_machines)

    @st.cache_data
    def get_data(table_name: str, machine: str, start: dt.date, end: dt.date, _cols_key: tuple):
        t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
        q = sa.select(t).where(
            t.c.機種 == machine,
            t.c.date.between(start, end)
        ).order_by(t.c.date, t.c.台番号)
        return pd.read_sql(q, eng)

    df = get_data(table_name, machine_sel, vis_start, vis_end, needed_cols)
    if df.empty:
        st.warning("データがありません"); st.stop()

    # URLへ現在の状態を反映
    st.query_params.update({"table": table_name, "machine": machine_sel})

    # KPIカード（期間全体または当日）
    dfr = df[df["date"] == vis_end] if vis_start == vis_end else df
    c1, c2, c3 = st.columns(3)
    c1.metric("平均 合成確率(実数)", f'{dfr["合成確率"].mean():.4f}' if not dfr.empty else "-")
    c2.metric("対象台数", dfr["台番号"].nunique() if "台番号" in dfr.columns else 0)
    if not dfr.empty:
        best_row = dfr.loc[dfr["合成確率"].idxmax()]
        c3.metric("ベスト台 (合成)", f'台{int(best_row["台番号"])}')
    else:
        c3.metric("ベスト台 (合成)", "-")

    # 表示オプション
    fmt_as_fraction = st.toggle("Y軸を 1/◯ 表示にする", value=True)
    use_hot_bg = st.toggle("“熱い日”背景ハイライト（5・7・土日）", value=False)
    use_downsample = st.toggle("長期間は週平均で表示（軽量化）", value=False)
    show_multi = st.checkbox("複数台を比較する", value=False)

    # 設定ライン
    thresholds = setting_map.get(machine_sel, {})
    df_rules = pd.DataFrame([{"setting": k, "value": v} for k, v in thresholds.items()]) \
        if thresholds else pd.DataFrame(columns=["setting","value"])

    # 背景
    def build_hot_background(start_d, end_d):
        df_bg = pd.DataFrame({"date": pd.date_range(start_d, end_d, freq="D")})
        df_bg["is_hot"] = df_bg["date"].apply(lambda d: (d.day in (5, 7)) or (d.weekday() >= 5))
        return alt.Chart(df_bg).mark_rect(opacity=0.08).encode(
            x="date:T",
            color=alt.condition("datum.is_hot", alt.value("red"), alt.value("transparent"), legend=None)
        )

    # Y軸
    if fmt_as_fraction:
        y_axis = alt.Axis(
            title="合成確率",
            labelExpr="isValid(datum.value) ? (datum.value==0 ? '0' : '1/'+format(round(1/datum.value),'d')) : ''"
        )
        tip_fmt = ".4f"
    else:
        y_axis = alt.Axis(title="合成確率(実数)", format=".4f")
        tip_fmt = ".4f"

    # タブ構成（比較は既定OFF）
    if show_multi:
        tab_avg, tab_single, tab_multi = st.tabs(["全台平均", "単台", "複数台比較"])
    else:
        tab_avg, tab_single = st.tabs(["全台平均", "単台"])
        tab_multi = None

    # ---------- 全台平均 ----------
    with tab_avg:
        df_avg = (
            df.groupby("date", as_index=False)["合成確率"]
              .mean()
              .rename(columns={"合成確率": "plot_val"})
        )
        if use_downsample:
            df_avg = df_avg.set_index("date").resample("W")["plot_val"].mean().reset_index()

        base = alt.Chart(df_avg).mark_line().encode(
            x="date:T",
            y=alt.Y("plot_val:Q", axis=y_axis),
            tooltip=[alt.Tooltip("date:T", title="日付"),
                     alt.Tooltip("plot_val:Q", title="値 (0=欠損含む)", format=tip_fmt)]
        ).properties(height=420)

        chart = base
        if not df_rules.empty:
            rules = alt.Chart(df_rules).mark_rule(strokeDash=[4, 2]).encode(
                y="value:Q",
                color=alt.Color("setting:N", legend=alt.Legend(title="設定ライン"))
            )
            chart = chart + rules
        if use_hot_bg:
            chart = build_hot_background(vis_start, vis_end) + chart

        st.subheader(f"📈 全台平均 合成確率 | {machine_sel}")
        st.altair_chart(chart, use_container_width=True)

    # ---------- 単台 ----------
    @st.cache_data
    def get_slots(table_name: str, machine: str, start: dt.date, end: dt.date, _cols_key: tuple):
        t = sa.Table(table_name, sa.MetaData(), autoload_with=eng)
        q = sa.select(t.c.台番号).where(
            t.c.機種 == machine, t.c.date.between(start, end)
        ).distinct().order_by(t.c.台番号)
        with eng.connect() as conn:
            vals = [r[0] for r in conn.execute(q) if r[0] is not None]
        return [int(v) for v in vals]

    slots_all = get_slots(table_name, machine_sel, vis_start, vis_end, needed_cols)

    with tab_single:
        if not slots_all:
            st.info("台番号のデータがありません")
        else:
            slot_sel = st.selectbox("台番号を選択", slots_all)
            df_single = df[df["台番号"] == slot_sel].rename(columns={"合成確率":"plot_val"})
            if use_downsample:
                df_single = (df_single.set_index("date")
                             .resample("W")["plot_val"].mean().reset_index())
            base = alt.Chart(df_single).mark_line().encode(
                x="date:T",
                y=alt.Y("plot_val:Q", axis=y_axis),
                tooltip=[alt.Tooltip("date:T", title="日付"),
                         alt.Tooltip("plot_val:Q", title="値 (0=欠損含む)", format=tip_fmt)]
            ).properties(height=420)

            chart = base
            if not df_rules.empty:
                rules = alt.Chart(df_rules).mark_rule(strokeDash=[4, 2]).encode(
                    y="value:Q",
                    color=alt.Color("setting:N", legend=alt.Legend(title="設定ライン"))
                )
                chart = chart + rules
            if use_hot_bg:
                chart = build_hot_background(vis_start, vis_end) + chart

            st.subheader(f"📈 合成確率 | {machine_sel} | 台 {slot_sel}")
            st.altair_chart(chart, use_container_width=True)

    # ---------- 複数台比較（任意） ----------
    if tab_multi is not None:
        with tab_multi:
            if not slots_all:
                st.info("台番号のデータがありません")
            else:
                default_slots = slots_all[:min(3, len(slots_all))]
                compare_slots = st.multiselect("比較する台番号（最大6台）",
                                               options=slots_all, default=default_slots, max_selections=6)
                if compare_slots:
                    df_multi = df[df["台番号"].isin(compare_slots)].rename(columns={"合成確率":"plot_val"})
                    if use_downsample:
                        df_multi = (df_multi.set_index("date")
                                    .groupby("台番号")["plot_val"].resample("W").mean()
                                    .reset_index())
                    base = alt.Chart(df_multi).mark_line().encode(
                        x="date:T",
                        y=alt.Y("plot_val:Q", axis=y_axis),
                        color=alt.Color("台番号:N", legend=alt.Legend(title="台番号")),
                        tooltip=[alt.Tooltip("date:T", title="日付"),
                                 alt.Tooltip("台番号:N", title="台"),
                                 alt.Tooltip("plot_val:Q", title="値 (0=欠損含む)", format=tip_fmt)]
                    ).properties(height=420)

                    chart = base
                    if not df_rules.empty:
                        rules = alt.Chart(df_rules).mark_rule(strokeDash=[4, 2]).encode(
                            y="value:Q",
                            color=alt.Color("setting:N", legend=alt.Legend(title="設定ライン"))
                        )
                        chart = chart + rules
                    if use_hot_bg:
                        chart = build_hot_background(vis_start, vis_end) + chart

                    st.subheader(f"📈 合成確率 比較 | {machine_sel} | 台 {', '.join(map(str, compare_slots))}")
                    st.altair_chart(chart, use_container_width=True)
                else:
                    st.info("台番号を選択してください。")

    # ========== 抽出結果のダウンロード ==========
    cols_basic = ["date", "機種", "台番号", "累計スタート", "BB回数", "RB回数", "合成確率"]
    dl_cols_mode = st.radio("ダウンロード列", ["基本セット", "すべて"], horizontal=True)
    out_df = df[cols_basic] if (dl_cols_mode == "基本セット" and all(c in df.columns for c in cols_basic)) else df
    csv_bytes = out_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button(
        "この期間・機種のデータをCSVでダウンロード",
        data=csv_bytes,
        file_name=f"{table_name}_{machine_sel}_{vis_start}_{vis_end}.csv",
        mime="text/csv"
    )
