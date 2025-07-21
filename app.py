import streamlit as st
import json, pandas as pd
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

st.title("🛠 Slot Data Manager (placeholder)")
st.write("Google Drive 接続テスト用アプリです。")

sa_dict = dict(st.secrets["gcp_service_account"])
st.write("✅ keys:", list(sa_dict.keys()))   # 必要キーが全部見えれば OK

creds = Credentials.from_service_account_info(
    sa_dict, scopes=["https://www.googleapis.com/auth/drive.readonly"]
)
st.success("Google Drive 認証 OK!")
