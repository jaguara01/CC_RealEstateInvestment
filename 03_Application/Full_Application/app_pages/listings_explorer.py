# pages/listings_explorer.py

import streamlit as st
from modules.data_loaders import load_listing_data
from modules.utils import format_currency
from streamlit_folium import st_folium
import folium
import plotly.express as px
from sklearn.preprocessing import MinMaxScaler

def render():
    """Render the Listings Explorer page: map, analytics & table."""
    st.title("Listings Explorer")
    st.markdown("Filter and drill into property listings pulled from S3.")

    # 1. Load data once (cached)
    with st.spinner("Loading property listings..."):
        df = load_listing_data()

    # 2. Sidebar: filters
    st.sidebar.header("Filters")
    # Price range filter
    min_price, max_price = st.sidebar.slider(
        "Price (€)",
        int(df.PRICE.min()), int(df.PRICE.max()),
        (int(df.PRICE.quantile(0.05)), int(df.PRICE.quantile(0.95)))
    )
    # Unit price filter
    min_unit, max_unit = st.sidebar.slider(
        "Unit price (€/m²)",
        int(df.UNITPRICE.min()), int(df.UNITPRICE.max()),
        (int(df.UNITPRICE.quantile(0.05)), int(df.UNITPRICE.quantile(0.95)))
    )
    # Rooms & baths
    rooms = st.sidebar.slider("Rooms", int(df.ROOMNUMBER.min()), int(df.ROOMNUMBER.max()), (1, 3))
    baths = st.sidebar.slider("Baths", int(df.BATHNUMBER.min()), int(df.BATHNUMBER.max()), (1, 2))

    # 3. Apply filters
    mask = (
        df.PRICE.between(min_price, max_price) &
        df.UNITPRICE.between(min_unit, max_unit) &
        df.ROOMNUMBER.between(*rooms) &
        df.BATHNUMBER.between(*baths)
    )
    filtered = df[mask]

    # 4. Tabs: Map / Analytics / Table
    tab1, tab2, tab3 = st.tabs(["Map view", "Analytics", "Data table"])

    # --- Map view ---
    with tab1:
        st.subheader("Interactive Map")
        if filtered.empty:
            st.warning("No listings match your filters.")
        else:
            m = folium.Map(
                location=[filtered.LATITUDE.mean(), filtered.LONGITUDE.mean()],
                zoom_start=12
            )
            for _, row in filtered.sample(min(len(filtered), 300), random_state=1).iterrows():
                folium.CircleMarker(
                    [row.LATITUDE, row.LONGITUDE],
                    radius=4,
                    color="blue",
                    fill=True,
                    popup=f"€{row.PRICE:,.0f} — {row.NEIGHBORHOOD}"
                ).add_to(m)
            st_folium(m, width=700, height=500)

    # --- Analytics ---
    with tab2:
        st.subheader("Price & Unit-Price Distributions")
        if filtered.empty:
            st.warning("No data available for analytics.")
        else:
            c1, c2 = st.columns(2)
            with c1:
                st.plotly_chart(px.histogram(filtered, x="PRICE", nbins=40), use_container_width=True)
            with c2:
                st.plotly_chart(px.histogram(filtered, x="UNITPRICE", nbins=40), use_container_width=True)

            st.subheader("Top Neighborhoods by Listing Count")
            summary = (
                filtered
                .groupby("NEIGHBORHOOD")
                .agg(count=("ASSETID", "count"),
                     avg_price=("PRICE", "mean"),
                     avg_unit=("UNITPRICE", "mean"))
                .sort_values("count", ascending=False)
            )
            top_n = st.slider("Show Top N", 5, 20, 10)
            st.bar_chart(summary.head(top_n)[["count"]])

    # --- Data table ---
    with tab3:
        st.subheader("Raw Listings Table")
        st.dataframe(filtered.reset_index(drop=True), use_container_width=True)

    # 5. Summary metrics
    st.markdown("---")
    st.write("### Summary Metrics")
    col1, col2, col3 = st.columns(3)
    col1.metric("Listings Shown", len(filtered))
    col2.metric("Avg. Price", format_currency(filtered.PRICE.mean() if not filtered.empty else None))
    col3.metric("Avg. Unit Price", format_currency(filtered.UNITPRICE.mean() if not filtered.empty else None))
