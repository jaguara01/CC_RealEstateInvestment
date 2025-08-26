# pages/sentiment_analysis.py

import streamlit as st
import plotly.express as px

from modules.config import SENT_BUCKET, SENT_PREFIX
from modules.sentiment import load_sentiment_posts, get_sentiment_model
from modules.utils import format_currency, format_percentage

def render():
    """Render the BlueSky sentiment‐analysis dashboard."""
    st.title("BlueSky Real Estate Sentiment")
    st.markdown("Analyze sentiment on BlueSky posts mentioning real estate keywords.")

    # 1. Load & preprocess posts (cached)
    with st.spinner("🔄 Loading posts from S3..."):
        df = load_sentiment_posts(bucket=SENT_BUCKET, prefix=SENT_PREFIX)

    if df.empty:
        st.warning("No posts found in S3.")
        return

    # 2. Run sentiment model (cached)
    model = get_sentiment_model()
    df = df[df["descripcion"].notnull()].copy()
    df["sentimiento"] = df["descripcion"].apply(lambda txt: model(txt)[0]["label"])

    st.success(f"✅ Processed {len(df)} posts")

    # 3. Key metrics
    col1, col2, col3 = st.columns(3)
    col1.metric("Accounts", df["account"].nunique())
    col2.metric("Real Estate Mentions", int(df["menciona"].sum()))
    pos = (df["sentimiento"] == "POSITIVE").sum()
    neg = (df["sentimiento"] == "NEGATIVE").sum()
    col3.metric("Positive | Negative", f"{pos} | {neg}")

    # 4. Sentiment distribution pie chart
    st.subheader("Sentiment Distribution")
    dist = df["sentimiento"].value_counts().reset_index()
    dist.columns = ["sentimiento", "count"]
    fig1 = px.pie(
        dist,
        values="count",
        names="sentimiento",
        color="sentimiento",
        color_discrete_map={
            "POSITIVE": "#A5D6A7",
            "NEGATIVE": "#EF9A9A",
            "NEUTRAL":  "#FFD54F"
        }
    )
    st.plotly_chart(fig1, use_container_width=True)

    # 5. Posts by account (bar chart)
    st.subheader("Top Accounts by Post Count")
    top_acc = df["account"].value_counts().head(10).reset_index()
    top_acc.columns = ["account", "count"]
    fig2 = px.bar(top_acc, x="account", y="count", text="count")
    st.plotly_chart(fig2, use_container_width=True)

    # 6. Sentiment breakdown per top-5 accounts (stacked bar)
    st.subheader("Sentiment by Account (Top 5)")
    top5 = top_acc["account"].tolist()
    df5 = df[df["account"].isin(top5)]
    breakdown = (
        df5.groupby(["account", "sentimiento"])
           .size()
           .reset_index(name="count")
    )
    fig3 = px.bar(
        breakdown,
        x="account",
        y="count",
        color="sentimiento",
        barmode="stack",
    )
    st.plotly_chart(fig3, use_container_width=True)

    # 7. Featured posts as cards
    st.subheader("Featured BlueSky Posts")
    emojis = {"POSITIVE": "😊", "NEGATIVE": "😡", "NEUTRAL": "😐"}
    colors = {
        "POSITIVE": "#81C784",  # Green 400
        "NEGATIVE": "#E57373",  # Red 400
        "NEUTRAL":  "#FFD54F",  # Amber 300
    }
    posts = df.head(12).to_dict(orient="records")
    cols = st.columns(3)

    for idx, p in enumerate(posts):
        with cols[idx % 3]:
            bgcolor = colors.get(p["sentimiento"], "#f7f7f7")
            emoji = emojis.get(p["sentimiento"], "")
            st.markdown(
                f"""
                <div style="
                    background-color: {bgcolor};
                    padding: 16px;
                    border-radius: 10px;
                    box-shadow: 0 2px 6px rgba(0, 0, 0, 0.1);
                    color: #111;
                    font-family: sans-serif;
                    margin: 8px;
                ">
                <h5 style="margin-bottom: 8px;">{emoji} {p['sentimiento'].capitalize()}</h5>
                <p style="margin: 4px 0;"><strong>Account:</strong> {p['account']}</p>
                <hr style="margin: 6px 0;">
                <p style="font-size: 14px; margin: 0;">{p['descripcion']}</p>
                </div>
                """,
                unsafe_allow_html=True
            )

    # 8. Download filtered posts
    st.markdown("---")
    df_mentions = df[df["menciona"]]
    csv = df_mentions.to_csv(index=False).encode("utf-8")
    st.download_button(
        "📥 Download Real Estate Mentions CSV",
        data=csv,
        file_name="blue_sky_real_estate_posts.csv",
        mime="text/csv"
    )
