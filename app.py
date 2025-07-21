import io, pandas as pd, streamlit as st
from src.drive_client import list_csv_files, download_file
from src.etl import normalize
from src.db import init_db, upsert, get_conn
from src.parse_meta import parse_meta

st.set_page_config(page_title="Slot Data Manager / Visualizer", layout="wide")
st.title("🎛️ Slot Data Manager & Visualizer")

# ---------- 共通：DB 初期化 & サイドバーでモード選択 ----------
init_db()                                   # テーブル無ければ作る
mode = st.sidebar.radio(
    "モードを選択",
    options=("📥 データ取り込み", "📊 データ可視化"),
    index=0,
)

# ---------- 1) データ取り込み UI ----------
if mode == "📥 データ取り込み":
    st.header("Google Drive → DB インポート")
    
    DEFAULT_FOLDER_ID = "1hX8GQRuDm_E1A1Cu_fXorvwxv-XF7Ynl"
    FOLDER_ID = st.text_input("Google Drive フォルダ ID", value=DEFAULT_FOLDER_ID)
    if st.button("📂 CSV 一覧を取得") and FOLDER_ID:
        files = list_csv_files_recursive(FOLDER_ID)
        if not files:
            st.warning("CSV が見つかりませんでした")
            st.stop()

        file_df = pd.DataFrame(files)[["name", "modifiedTime", "size"]]
        st.dataframe(file_df, height=300)

        selected = st.multiselect(
            "取り込むファイルを選択",
            options=file_df.index,
            format_func=lambda i: file_df.loc[i, "name"],
        )

        if st.button("🚀 選択した CSV を取り込む", disabled=not selected):
            bar = st.progress(0.0)
            for idx, i in enumerate(selected, 1):
                meta = files[i]
                raw = download_file(meta["id"])
                df_raw = pd.read_csv(io.BytesIO(raw), encoding="shift_jis")

                store, machine, date = parse_meta(meta["name"])
                df = normalize(df_raw, store)
                df["store"], df["machine"] = store, machine
                df["date"] = pd.to_datetime(date).date()

                upsert(df)
                bar.progress(idx / len(selected), f"{idx}/{len(selected)}")
            st.success(f"✅ {len(selected)} 件インポート完了！")

# ---------- 2) データ可視化 UI ----------
else:
    st.header("DB から可視化")

    conn = get_conn()

    stores = conn.query("SELECT DISTINCT store FROM slot_data")["store"].tolist()
    if not stores:
        st.info("まだデータがありません。まず『データ取り込み』で CSV を入れてください。")
        st.stop()

    store = st.sidebar.selectbox("店舗", stores)

    machines = conn.query(
        "SELECT DISTINCT machine FROM slot_data WHERE store = :s",
        params={"s": store})["machine"].tolist()
    machine = st.sidebar.selectbox("機種", machines)

    metric = st.sidebar.selectbox("表示項目", ["合成確率", "BB確率", "RB確率"])

    sql = f"""
    SELECT date, 台番号, {metric}
      FROM slot_data
     WHERE store = :store AND machine = :machine
     ORDER BY date
    """
    df = conn.query(sql, params=dict(store=store, machine=machine))

    if df.empty:
        st.warning("該当データがありません")
    else:
        pivot = df.pivot(index="date", columns="台番号", values=metric)
        st.line_chart(pivot)
