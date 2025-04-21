"""
config.py – central place to pull API keys

Usage:
    from src.config import secret
    token = secret("APIFY_TOKEN")
"""

import os

def secret(key: str) -> str:
    # Works both inside Streamlit and in plain Python
    try:
        import streamlit as st
        if key in st.secrets:          # type: ignore[attr-defined]
            return st.secrets[key]
    except ModuleNotFoundError:
        pass
    return os.environ[key]
