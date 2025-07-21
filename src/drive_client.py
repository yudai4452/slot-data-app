# src/drive_client.py
import io, streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

# ---------- 認証ハンドル ----------
@st.cache_resource
def get_drive():
    scopes = ["https://www.googleapis.com/auth/drive.readonly"]
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"], scopes=scopes
    )
    return build("drive", "v3", credentials=creds)

drive = get_drive()

# ---------- 再帰で .csv を全部集める ----------
def list_csv_files_recursive(folder_id: str):
    """
    指定フォルダ以下のサブフォルダをすべて探索し、
    名前が .csv で終わるファイルを返す。
    戻り値: [{'id': ..., 'name': ..., 'mimeType': ...}, ...]
    """
    all_files, queue = [], [folder_id]
    while queue:
        fid = queue.pop()
        res = drive.files().list(
            q=f"'{fid}' in parents and trashed = false",
            fields="files(id, name, mimeType)",
            pageSize=1000,
            supportsAllDrives=True,
        ).execute()
        for f in res.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                queue.append(f["id"])                            # サブフォルダは再探索
            elif f["name"].lower().endswith(".csv"):
                all_files.append(f)
    return all_files

# ---------- 1 ファイルを bytes で取得 ----------
def download_file(file_id: str) -> bytes:
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, request).next_chunk()
    buf.seek(0)
    return buf.read()
