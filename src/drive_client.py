# src/drive_client.py  ★全置き換え

import io, streamlit as st
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload

@st.cache_resource
def _drive():
    creds = Credentials.from_service_account_info(
        st.secrets["gcp_service_account"],
        scopes=["https://www.googleapis.com/auth/drive.readonly"],
    )
    return build("drive", "v3", credentials=creds)

drive = _drive()

def list_csv_files_recursive(folder_id: str, cur_path=""):
    """フォルダ以下すべて探索して .csv を返す（path 付き）"""
    out, queue = [], [(folder_id, cur_path)]
    while queue:
        fid, path = queue.pop()
        res = drive.files().list(
            q=f"'{fid}' in parents and trashed=false",
            fields="files(id,name,mimeType)",
            pageSize=1000, supportsAllDrives=True).execute()
        for f in res.get("files", []):
            if f["mimeType"] == "application/vnd.google-apps.folder":
                queue.append((f["id"], f"{path}/{f['name']}"))
            elif f["name"].lower().endswith(".csv"):
                out.append({**f, "path": f"{path}/{f['name']}"})
    return out

def download_file(file_id: str) -> bytes:
    req = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    MediaIoBaseDownload(buf, req).next_chunk()
    buf.seek(0)
    return buf.read()
