#Explotation Zone: Unstructured Data BDM Project - Group 11_A

import streamlit as st
import pandas as pd
import boto3
import os
import json
from dotenv import load_dotenv
from transformers import pipeline
import plotly.express as px

# --- CONFIGURATION ---
load_dotenv()
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.getenv("S3_BUCKET", "stream-worker-files")
PREFIX = "posts_bluesky/"
KEYWORDS = ["venta", "apartamento", "casa", "pisos", "inmueble", "inmobiliaria", "barcelona", "bcn", "compra", "renta"]

def cargar_jsons(bucket, prefix):
    s3 = boto3.client("s3", aws_access_key_id=AWS_ACCESS_KEY, aws_secret_access_key=AWS_SECRET_KEY)
    objects = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    data = []
    for obj in objects.get("Contents", []):
        if obj["Key"].endswith(".json"):
            raw = s3.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            data.append(json.loads(raw))
    return data

def procesar(datos):
    rows = []
    for post in datos:
        rows.append({
            "account": post.get("author", {}).get("handle"),
            "descripcion": post.get("text"),
            "likes": post.get("metrics", {}).get("likes", 0),
            "reposts": post.get("metrics", {}).get("reposts", 0),
            "replies": post.get("metrics", {}).get("replies", 0),
        })
    df = pd.DataFrame(rows)
    df["menciona_inmuebles"] = df["descripcion"].str.lower().apply(lambda x: any(k in x for k in KEYWORDS) if isinstance(x, str) else False)
    return df

@st.cache_resource
def cargar_modelo():
    return pipeline("sentiment-analysis", model="distilbert-base-uncased-finetuned-sst-2-english")

st.set_page_config(page_title="🏘️ Real Estate + BlueSky Sentiment Dashboard", layout="wide")
st.title("🏘️ Real Estate Sentiment Analysis on BlueSky 📊")

with st.spinner("🔄 Loading posts from S3..."):
    datos = cargar_jsons(S3_BUCKET, PREFIX)
    df = procesar(datos)

modelo = cargar_modelo()
df = df[df["descripcion"].notnull()].copy()
df["sentimiento"] = df["descripcion"].apply(lambda t: modelo(t)[0]["label"])

st.success(f"✅ {len(df)} posts processed from S3")

# METRICS
col1, col2, col3 = st.columns(3)
col1.metric("🧑‍💼 Accounts", df["account"].nunique())
col2.metric("🏡 Real estate posts", df["menciona_inmuebles"].sum())
col3.metric("😃 Positive | 😡 Negative", f'{(df["sentimiento"]=="POSITIVE").sum()} | {(df["sentimiento"]=="NEGATIVE").sum()}')

# PIE CHART
st.subheader("📊 Sentiment Distribution")
sent_counts = df["sentimiento"].value_counts().reset_index()
sent_counts.columns = ["sentimiento", "count"]
fig = px.pie(sent_counts, values="count", names="sentimiento",
             color_discrete_map={"POSITIVE": "#4CAF50", "NEGATIVE": "#F44336", "NEUTRAL": "#FFC107"})
st.plotly_chart(fig, use_container_width=True)

# BAR CHART
st.subheader("👤 Posts by Account")
top_accounts = df["account"].value_counts().head(10).reset_index()
top_accounts.columns = ["account", "count"]
fig_accounts = px.bar(top_accounts, x="account", y="count", color="count", color_continuous_scale="Blues")
st.plotly_chart(fig_accounts, use_container_width=True)

# STACKED BARS
st.subheader("📊 Sentiments by Account (Top 5)")
top5 = df["account"].value_counts().head(5).index.tolist()
df_top = df[df["account"].isin(top5)]
df_grouped = df_top.groupby(["account", "sentimiento"]).size().reset_index(name="count")
fig_stacked = px.bar(df_grouped, x="account", y="count", color="sentimiento", barmode="stack",
                     color_discrete_map={"POSITIVE": "#4CAF50", "NEGATIVE": "#F44336", "NEUTRAL": "#FFC107"})
st.plotly_chart(fig_stacked, use_container_width=True)

# CARDS
st.subheader("🧾 Featured BlueSky Posts")
emojis = {"POSITIVE": "😊", "NEGATIVE": "😡", "NEUTRAL": "😐"}
colors = {"POSITIVE": "#e8f5e9", "NEGATIVE": "#ffebee", "NEUTRAL": "#fff8e1"}
rows = df.head(12).to_dict(orient="records")
cols = st.columns(3)
for idx, row in enumerate(rows):
    with cols[idx % 3]:
        st.markdown(f"""
        <div style=\"background-color:{colors.get(row['sentimiento'], '#f0f0f0')}; padding:16px; border-radius:10px; margin-bottom:20px\">
            <h5 style=\"margin-bottom:5px;\">{emojis.get(row['sentimiento'], '')} {row['sentimiento'].capitalize()}</h5>
            <p><strong>Account:</strong> {row['account']}</p>
            <hr style=\"margin-top:5px;margin-bottom:5px;\">
            <p style=\"font-size:15px;\">{row['descripcion']}</p>
        </div>
        """, unsafe_allow_html=True)

# DOWNLOAD
inmuebles_df = df[df["menciona_inmuebles"]]
csv = inmuebles_df.to_csv(index=False).encode("utf-8")
st.download_button("📥 Download real estate posts", data=csv, file_name="real_estate_posts.csv")
