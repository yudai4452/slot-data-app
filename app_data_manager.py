import io, pandas as pd, streamlit as st
from src.drive_client import list_csv_files, download_file
from src.parse_meta import parse_meta
from src.etl import normalize
from src.db import init_db, upsert

st.set_page_config(page_title="Slot Data Manager", layout="wide")
st.title("🛠️ Slot Data Manager")

init_db()      # テーブル無ければ作る

FOLDER_ID = st.text_input("Google Drive フォルダ ID", key="folder")
list_btn = st.button("📂 CSV 一覧を取得")

if list_btn and FOLDER_ID:
    files = list_csv_files(FOLDER_ID)
    if not files:
        st.warning("CSV が見つかりませんでした")
        st.stop()

    # 一覧表示 + チェックボックスで選択
    file_df = pd.DataFrame(files).rename(columns={"name":"ファイル名","modifiedTime":"更新日時","size":"サイズ"})
    selected = st.multiselect("取り込むファイルを選択", options=file_df.index, format_func=lambda i: file_df.loc[i,"ファイル名"])
    if st.button("🚀 インポート実行", disabled=not selected):
        bar = st.progress(0.0)
        for idx, i in enumerate(selected, 1):
            fmeta = files[i]
            raw_bytes = download_file(fmeta["id"])
            df_raw = pd.read_csv(io.BytesIO(raw_bytes), encoding="shift_jis")

            store, machine, date = parse_meta(fmeta["name"])
            df_norm = normalize(df_raw, store)
            df_norm["store"], df_norm["machine"] = store, machine
            df_norm["date"] = pd.to_datetime(date).date()

            upsert(df_norm)
            bar.progress(idx / len(selected), text=f"{idx}/{len(selected)} done")
        st.success(f"✅ {len(selected)} 件インポート完了！")
