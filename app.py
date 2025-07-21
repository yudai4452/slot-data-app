import streamlit as st
import json, pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

st.title("🛠 Slot Data Manager (placeholder)")
st.write("Google Drive 接続テスト用アプリです。")

creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
drive = build("drive", "v3", credentials=creds)
st.success("✅ Google Drive 認証 OK!")
