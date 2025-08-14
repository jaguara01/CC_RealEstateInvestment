# main.py

import streamlit as st
from streamlit_option_menu import option_menu

# Import page modules
# from app_pages.home import render as render_home
from app_pages.listings_explorer import render as render_listings
from app_pages.financial_analyzer import render as render_financial_analyzer
# from app_pages.price_prediction import render as render_investment_predicter
from app_pages.sentiment_analysis import render as render_sentiment


# Map sidebar labels to render functions
PAGES = {
    "Listings Explorer": render_listings,
    "Financial Analyzer": render_financial_analyzer,
    # "Investment Recommender": render_investment_predicter,
    "Sentiment Analysis": render_sentiment,
}

def main():
    st.set_page_config(layout="wide")
    choice = option_menu(
        menu_title=None,  # hides the title
        options=["Listings", "Analyzer", "Sentiment"],
        icons=["house", "currency-dollar", "chat-quote"],
        menu_icon="cast",
        default_index=0,
        orientation="horizontal",
    )
    if "Listings" in choice:
        render_listings()
    elif "Analyzer" in choice:
        render_financial_analyzer()
    else:
        render_sentiment()

if __name__ == "__main__":
    main()
