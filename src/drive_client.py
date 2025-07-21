# src/drive_client.py
import io
import streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ❶ Drive API ハンドルをキャッシュ
@st.cache_resource
def get_drive():
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    return build("drive", "v3", credentials=creds)

drive = get_drive()

# ❷ 指定フォルダ直下の CSV を取得
def list_csv_files(folder_id: str):
    q = f"'{folder_id}' in parents and mimeType='text/csv'"
    res = drive.files().list(
        q=q,
        fields="files(id, name, size, modifiedTime)",
        pageSize=1000,
        supportsAllDrives=True,
    ).execute()
    return res.get("files", [])

# ❸ 1 ファイルを bytes としてダウンロード
def download_file(file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    fh = io.BytesIO()
    MediaIoBaseDownload(fh, request).next_chunk()
    fh.seek(0)
    return fh.read()
