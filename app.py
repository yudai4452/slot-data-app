# app.py  ★全置き換え

import io, datetime as dt, pandas as pd, streamlit as st
from src.drive_client import list_csv_files_recursive, download_file
from src.etl import normalize
from src.db import init_db, upsert, get_conn, latest_date_in_db
from src.parse_meta import parse_meta

DEFAULT_FOLDER_ID = "1hX8GQRuDm_E1A1Cu_fXorvwxv-XF7Ynl"   # ← ご指定フォルダ

st.set_page_config(page_title="Slot Data Manager & Visualizer", layout="wide")
st.title("🎛️ Slot Data Manager & Visualizer")

# ---------- 共通：DB 初期化 ----------
init_db()

# ---------- サイドバーでモード選択 ----------
mode = st.sidebar.radio("モード", ("📥 取り込み", "📊 可視化"))

# ▼▼▼ 取り込みモード ▼▼▼ --------------------------------------------------
if mode == "📥 取り込み":
    st.header("Google Drive → DB インポート")

    folder_id = st.text_input("Google Drive フォルダ ID", value=DEFAULT_FOLDER_ID)

# ---------- 🔍 スキャン + インポート UI ----------

# ① Drive をスキャンして結果を session_state に保存
if st.button("🔍 ファイル自動スキャン") and folder_id:
    files = list_csv_files_recursive(folder_id)
    st.session_state["scan_files"] = files
    st.session_state["scan_done"]  = True
    st.rerun()                      # ← すぐ再実行

# ② スキャン済みなら取り込み画面を表示
if st.session_state.get("scan_done"):
    files = st.session_state["scan_files"]

    # --- 日付レンジ入力 ---
    db_latest = latest_date_in_db()
    col1, col2 = st.columns(2)
    start_d = col1.date_input("Start date", value=db_latest or dt.date(2000, 1, 1))
    end_d   = col2.date_input("End date",   value=dt.date.today())

    # --- 日付でフィルタ ---
    target = []
    for f in files:
        date_str = f["name"][-14:-4]              # YYYY-MM-DD
        try:
            f_date = dt.date.fromisoformat(date_str)
        except ValueError:
            continue
        if start_d <= f_date <= end_d:
            target.append(f)

    st.write(f"🎯 対象 CSV: **{len(target)} 件**")

    # ③ インポート実行ボタン
    if st.button("🚀 一括インポート", key="import", disabled=not target):
        bar    = st.progress(0.0)
        status = st.empty()
        for i, meta in enumerate(target, 1):
            status.write(f"⏳ {meta['name']} …")
            raw = download_file(meta["id"])
            df_raw = pd.read_csv(io.BytesIO(raw), encoding="shift_jis")
            store, machine, date = parse_meta(meta["path"])
            df = normalize(df_raw, store)
            df["store"], df["machine"], df["date"] = store, machine, date
            upsert(df)
            bar.progress(i / len(target))
        status.write("✅ 完了！")
        st.success(f"{len(target)} 件インポートしました")
        st.session_state.pop("scan_done")   # 次回のためにリセット

# ▲▲▲ 取り込みモードここまで ▲▲▲ ----------------------------------------

# ▼▼▼ 可視化モード ▼▼▼ ----------------------------------------------------
else:
    st.header("DB から可視化")

    conn = get_conn()
    stores = conn.query("SELECT DISTINCT store FROM slot_data")["store"].tolist()
    if not stores:
        st.info("まず『取り込み』タブでデータを入れてください。")
        st.stop()

    store = st.sidebar.selectbox("店舗", stores)
    machines = conn.query(
        "SELECT DISTINCT machine FROM slot_data WHERE store=:s",
        params={"s": store})["machine"].tolist()
    machine = st.sidebar.selectbox("機種", machines)
    metric = st.sidebar.selectbox("指標", ["合成確率", "BB確率", "RB確率"])

    sql = f"""
      SELECT date, 台番号, {metric}
        FROM slot_data
       WHERE store=:store AND machine=:machine
       ORDER BY date
    """
    df = conn.query(sql, params=dict(store=store, machine=machine))
    if df.empty:
        st.warning("データがありません")
    else:
        st.line_chart(df.pivot(index="date", columns="台番号", values=metric))
# ▲▲▲ 可視化モードここまで ▲▲▲ ------------------------------------------
