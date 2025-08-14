import streamlit as st
from io import StringIO
import pandas as pd
import numpy as np
import plotly.express as px
import folium
import time
from streamlit_folium import st_folium
from sklearn.preprocessing import MinMaxScaler
import boto3

# Configuration & Caching
st.set_page_config(
    page_title="InmoDecision",
    layout="wide",
    initial_sidebar_state="expanded",
)

@st.cache_data(show_spinner=False)
def load_listing_data():
    # idealista listings from s3
    s3_bucket = "01-structured"
    s3_key = "property-listings/idealista/Barcelona_Sale_enriched.csv"
    
    # init s3 client
    s3 = boto3.client("s3")
    
    # read the file into memory
    response = s3.get_object(Bucket=s3_bucket, Key=s3_key)
    content = response["Body"].read().decode("utf-8")
    df = pd.read_csv(StringIO(content), low_memory=False)

    df["LATITUDE"] = pd.to_numeric(df.get("LATITUDE"), errors="coerce")
    df["LONGITUDE"] = pd.to_numeric(df.get("LONGITUDE"), errors="coerce")
    df = df.dropna(subset=["LATITUDE", "LONGITUDE"])

    if "NEIGHBORHOOD" not in df.columns:
        if "ADDRESS" in df.columns:
            df["NEIGHBORHOOD"] = df["ADDRESS"].str.split(pat=",", n=1).str[0]
        else:
            df["NEIGHBORHOOD"] = "Unknown"

    return df

@st.cache_data(show_spinner=False)
def load_poi_data():
    """
    Load points of interest
    """
    pois = pd.read_csv("data/Barcelona_POIS.csv")
    return pois

# Financial analysis logic
def real_estate_investment_analysis(
    property_value=None,
    property_price=500000,
    legal_buying_fees=30000,
    initial_fees=10000,
    down_payment=100000,
    interest_rate=0.03,
    loan_term_years=25,
    annual_appreciation_rate=0.035,
    monthly_rent=2000,
    monthly_expenses_percent=0.20,
):

    yearly_data = pd.DataFrame()
    monthly_data = pd.DataFrame()
    investment_summary = {}
    breakeven_month = None
    return yearly_data, monthly_data, investment_summary, breakeven_month

# Utility functions
def format_currency(value):
    if pd.isna(value): return "N/A"
    if np.isinf(value): return "Infinity"
    return f"€{value:,.0f}"

# Sidebar
st.sidebar.title("InmoDecision Settings")

if st.sidebar.button("Reload All Data"):
    # clear cached data
    st.cache_data.clear()
    st.warning("Data cache cleared. Please reload the page to apply changes.")

# Page navigation
def main():
    page = st.sidebar.radio(
        "Navigate to", 
        ["Home", "Map Explorer", "Financial Analyzer", "Recommendations"],
        index=0
    )
    if page == "Home":
        show_home()
    elif page == "Map Explorer":
        show_map()
    elif page == "Financial Analyzer":
        show_financial()
    elif page == "Recommendations":
        show_recommendations()

# Home Page
def show_home():
    st.title("InmoDecision: Data-Driven Real Estate Advisor")
    st.markdown("Combine open market listings to make smarter investments.")
    df = load_listing_data()
    st.metric("Total Listings Loaded", len(df))
    st.metric("Neighborhoods Covered", df['NEIGHBORHOOD'].nunique())
    last_refresh = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M")
    st.caption(f"Last data refresh: {last_refresh}")

# Map Explorer
def show_map():
    st.title("Map Explorer")
    df = load_listing_data()
    pois = load_poi_data()
    st.sidebar.header("Filter Listings")
    mask = pd.Series(True, index=df.index)
    # Numeric filters
    numeric_fields = [
        ('PRICE','Price (€)'),
        ('UNITPRICE','Unit price (€/m²)'),
        ('CONSTRUCTEDAREA','Area (m²)'),
        ('DISTANCE_TO_CENTER','Distance to center (km)')
    ]
    for col, label in numeric_fields:
        if col in df.columns:
            lo, hi = st.sidebar.slider(
                label,
                float(df[col].min()),
                float(df[col].max()),
                (float(df[col].min()), float(df[col].max()))
            )
            mask &= df[col].between(lo, hi)
    # Categorical
    rooms = st.sidebar.slider("Rooms", int(df.ROOMNUMBER.min()), int(df.ROOMNUMBER.max()), (1,5))
    mask &= df.ROOMNUMBER.between(*rooms)
    baths = st.sidebar.slider("Baths", int(df.BATHNUMBER.min()), int(df.BATHNUMBER.max()), (1,2))
    mask &= df.BATHNUMBER.between(*baths)
    # Amenities
    for col,label in [('HASTERRACE','Terrace'),('HASLIFT','Elevator'),('HASAIRCONDITIONING','AC'),('HASPARKINGSPACE','Parking')]:
        if st.sidebar.checkbox(label): mask &= df[col]==1
    # Period
    if 'PERIOD' in df.columns:
        sel = st.sidebar.selectbox("Period (yyyymm)", sorted(df.PERIOD.unique()), index=len(df.PERIOD.unique())-1)
        mask &= df.PERIOD==sel
    filtered = df[mask]
    # Tabs
    tab1, tab2, tab3 = st.tabs(["Map view","Analytics","Data table"])
    with tab1:
        st.subheader("Listings Map")
        if not filtered.empty:
            m = folium.Map(location=[filtered.LATITUDE.mean(), filtered.LONGITUDE.mean()], zoom_start=12)
            for _, r in filtered.sample(min(len(filtered),500), random_state=1).iterrows():
                folium.CircleMarker(
                    location=[r.LATITUDE, r.LONGITUDE], radius=5, fill=True,
                    popup=f"€{r.PRICE:,.0f} — {r.NEIGHBORHOOD}"
                ).add_to(m)
            st_folium(m, width=700, height=500)
        else:
            st.warning("No listings match the filters.")
    with tab2:
        st.subheader("Price & Size Distribution")
        if not filtered.empty:
            c1,c2 = st.columns(2)
            with c1:
                st.plotly_chart(px.histogram(filtered, x="PRICE", nbins=40), use_container_width=True)
            with c2:
                st.plotly_chart(px.histogram(filtered, x="UNITPRICE", nbins=40), use_container_width=True)
            st.subheader("Top Neighborhoods by Listings")
            summ = filtered.groupby('NEIGHBORHOOD', as_index=False).agg(
                avg_price=('PRICE','mean'), avg_unit=('UNITPRICE','mean'), count=('ASSETID','count')
            )
            topn = st.slider("Top N neighborhoods", 5,20,10)
            top = summ.nlargest(topn,'count').set_index('NEIGHBORHOOD')
            st.bar_chart(top[['avg_price','avg_unit']])
        else:
            st.warning("No data for analytics.")
    with tab3:
        st.subheader("Listings Table")
        st.dataframe(filtered.reset_index(drop=True), use_container_width=True)

# Financial Analyzer
def show_financial():
    st.title("Financial Analyzer")
    st.sidebar.header("Investment Parameters (€)")
    purchase = st.sidebar.number_input("Purchase Price", min_value=1, value=300000)
    fees_pct = st.sidebar.slider("Legal & Buying Fees (%)", 0.0,30.0,17.0)
    fees = purchase*(fees_pct/100)
    other_fees = st.sidebar.number_input("Other Initial Fees", 0,50000,1000)
    down_opt = st.sidebar.radio("Down Payment", ["% of total","Absolute amount"])
    total_cost = purchase+fees+other_fees
    if down_opt=='% of total':
        dp_pct = st.sidebar.slider("Down Payment (%)",0.1,100.0,20.0)
        dp = total_cost*(dp_pct/100)
    else:
        dp = st.sidebar.number_input("Down Payment (€)",1,int(total_cost*0.5))
    rate = st.sidebar.slider("Annual Interest Rate (%)",0.0,15.0,3.5)/100
    term = st.sidebar.slider("Loan Term (yrs)",1,40,25)
    rent = st.sidebar.number_input("Monthly Rent",0,5000,1500)
    exp_pct = st.sidebar.slider("Monthly Expenses (%)",0.0,100.0,20.0)/100
    appr = st.sidebar.slider("Annual Appreciation (%)",-5.0,15.0,3.5)/100
    if st.sidebar.button("Analyze Investment"):
        year_df, mon_df, summary, breakeven = real_estate_investment_analysis(
            property_price=purchase, legal_buying_fees=fees, initial_fees=other_fees,
            down_payment=dp, interest_rate=rate, loan_term_years=term,
            annual_appreciation_rate=appr, monthly_rent=rent, monthly_expenses_percent=exp_pct
        )
        st.session_state["fin_res"] = (year_df, mon_df, summary, breakeven)
    if "fin_res" in st.session_state:
        ydf, mdf, summ, be = st.session_state["fin_res"]
        st.header("Investment Summary")
        cols = st.columns(4)
        cols[0].metric("Down Payment", format_currency(summ.get('Down Payment (Initial Cash Investment)',0)))
        cols[0].metric("Property Price", format_currency(summ.get('Property Purchase Price',0)))
        cols[0].metric("Loan Amount", format_currency(summ.get('Loan Amount',0)))
        cols[1].metric("Monthly Payment", format_currency(summ.get('Monthly Loan Payment',0)))
        cols[1].metric("Net Rent", format_currency(summ.get('Monthly Net Rental Income',0)))
        cols[1].metric("Interest Rate", f"{summ.get('Annual Interest Rate (%)',0):.2f}%")
        cols[2].metric("Appreciation Rate", f"{summ.get('Annual Appreciation Rate (%)',0):.1f}%")
        cols[2].metric("Loan Term", f"{summ.get('Loan Term (Years)',0)} yrs")
        cols[2].metric("Total Cost", format_currency(summ.get('Total Initial Cost',0)))
        cols[3].metric("Breakeven Month", f"Month {be}" if be else "N/A")
        view = st.radio("View:", ["Annual","Monthly"], horizontal=True)
        if view=="Annual" and not ydf.empty:
            data = ydf.set_index('Year')
        elif view=="Monthly" and not mdf.empty:
            mdf['Total_Equity'] = mdf['Property_Value']-mdf['Remaining_Capital']
            data = mdf.set_index('Month')
        else:
            st.warning("No data for selected view.")
            return
        st.subheader(f"{view} Charts")
        c1,c2 = st.columns(2)
        with c1:
            st.line_chart(data.filter(regex='Property_Value|Remaining_Capital'))
            st.line_chart(data.filter(regex='Accumulated_Profit_If_Sold|Cumulative_Income_No_Sale'))
        with c2:
            st.bar_chart(data.filter(regex='Interest_Paid|Principal_Paid'))
            st.area_chart(data['Total_Equity'])
        st.subheader(f"{view} Data Table")
        st.dataframe(data.reset_index(), use_container_width=True)
        csv = data.reset_index().to_csv(index=False).encode('utf-8')
        st.download_button(f"Download {view} CSV", data=csv, file_name=f"{view}_projection.csv")

# Recommendations
def show_recommendations():
    st.title("Recommendations")
    df = load_listing_data()
    # avg price per neighborhood over city mean
    nb_stats = df.groupby('NEIGHBORHOOD')['PRICE'].mean()
    merged = nb_stats.to_frame('avg_price')
    merged['trend'] = merged['avg_price'].pct_change().fillna(0)
    scaler = MinMaxScaler()
    merged['trend_n'] = scaler.fit_transform(merged[['trend']])
    alpha = st.slider("Weight: Market Trend", 0.0,1.0,1.0)
    merged['score'] = alpha * merged['trend_n']
    top = merged.nlargest(5,'score')
    for nb, row in top.iterrows():
        st.markdown(f"**{nb}** — Trend: {row['trend']:.1%}, Score: {row['score']:.2f}")

if __name__ == "__main__":
    main()
