import streamlit as st

def poner_fondo(url_imagen):
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("{url_imagen}");
            background-attachment: fixed;
            background-size: cover;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
