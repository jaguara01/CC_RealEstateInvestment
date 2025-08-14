import streamlit as st
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.model_selection import train_test_split
from sklearn.impute import SimpleImputer
import numpy as np


# --- 1. Data Loading and Caching ---
@st.cache_data  # Cache the data loading to improve performance
def load_data(file_path):
    """Loads property data from a CSV file."""
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        st.error(
            f"Error: The file '{file_path}' was not found. Please make sure it's in the same directory."
        )
        return None
    except Exception as e:
        st.error(f"Error loading data: {e}")
        return None


# --- 2. Model Training ---
@st.cache_resource  # Cache the model to avoid retraining on every interaction
def train_price_model(df):
    """Trains a simple linear regression model to predict SalePrice."""
    if df is None or "SalePrice" not in df.columns:
        return None, None, None

    # Define features (numerical for simplicity) and target
    # These features are commonly influential and tend to be numerical.
    features = [
        "OverallQual",
        "GrLivArea",
        "TotalBsmtSF",
        "YearBuilt",
        "FullBath",
        "BedroomAbvGr",
        "GarageCars",
        "LotArea",
        "1stFlrSF",
        "TotRmsAbvGrd",
    ]

    # Filter out features not present in the dataframe
    available_features = [f for f in features if f in df.columns]
    if not available_features:
        st.warning(
            "None of the selected features for modeling are present in the uploaded data. Cannot train model."
        )
        return None, None, None

    X = df[available_features].copy()  # Make a copy to avoid SettingWithCopyWarning
    y = df["SalePrice"].copy()

    # Handle missing target values
    if y.isnull().any():
        st.warning(
            f"Warning: {y.isnull().sum()} rows with missing 'SalePrice' were dropped before training."
        )
        valid_indices = y.notnull()
        X = X[valid_indices]
        y = y[valid_indices]

    if X.empty or y.empty:
        st.error(
            "Not enough data to train the model after handling missing SalePrice values."
        )
        return None, None, None

    # Impute missing values in features using the median
    # This is a simple strategy for this basic model.
    for col in X.columns:
        if X[col].isnull().any():
            median_val = X[col].median()
            X.loc[:, col] = X[col].fillna(median_val)  # Use .loc to ensure assignment
            # st.info(f"Imputed missing values in '{col}' with median: {median_val:.2f}")

    # Train the model
    model = LinearRegression()
    try:
        model.fit(X, y)
    except Exception as e:
        st.error(f"Error during model training: {e}")
        return None, None, None

    return (
        model,
        available_features,
        X,
    )  # Return X to get feature values for selected property


# --- 3. Streamlit App UI ---
def run_app():
    st.set_page_config(layout="wide", page_title="Property Price Estimator")
    st.title("🏡 Simple Property Price Estimator")
    st.markdown(
        """
        This app uses a basic linear regression model to estimate property prices.
        Select a property from the sidebar to see its details and price analysis.
        The "market value" is based on the model's prediction using the available data.
    """
    )

    # File uploader in the sidebar
    st.sidebar.header("Upload Your Data")
    uploaded_file = st.sidebar.file_uploader(
        "Upload 'original_listing.csv'", type=["csv"]
    )

    if uploaded_file is not None:
        property_data = load_data(uploaded_file)
    else:
        # Attempt to load a default file if no upload (useful for initial run or if file is always in same dir)
        # For this example, we expect an upload.
        st.info("Please upload your 'original_listing.csv' file to begin.")
        property_data = None

    if property_data is not None:
        if "Id" not in property_data.columns:
            st.error(
                "The uploaded CSV must contain an 'Id' column to select properties."
            )
            return
        if "SalePrice" not in property_data.columns:
            st.error(
                "The uploaded CSV must contain a 'SalePrice' column for the target variable."
            )
            return

        model, features_used_in_model, X_processed = train_price_model(
            property_data.copy()
        )  # Pass a copy for safety

        if model and features_used_in_model:
            st.sidebar.header("Select Property")
            # Ensure IDs are unique and handle potential non-numeric or NaN IDs
            property_ids = property_data["Id"].dropna().unique().tolist()
            if not property_ids:
                st.sidebar.warning("No valid 'Id' values found in the data.")
                return

            try:
                # Attempt to sort IDs, assuming they might be numeric or string representations of numbers
                sorted_property_ids = sorted(
                    property_ids, key=lambda x: int(x) if str(x).isdigit() else str(x)
                )
            except ValueError:
                # Fallback to simple sort if conversion to int fails for some IDs
                sorted_property_ids = sorted(property_ids, key=str)

            selected_id = st.sidebar.selectbox(
                "Choose a Property ID:", options=sorted_property_ids
            )

            if selected_id:
                # Get the original row for display
                selected_property_original = property_data[
                    property_data["Id"] == selected_id
                ].iloc[0]

                # Get the processed features for prediction (from X_processed which has imputed values)
                # Find the index in the original dataframe that corresponds to the selected_id
                original_index = property_data[
                    property_data["Id"] == selected_id
                ].index[0]

                if original_index in X_processed.index:
                    property_features_for_prediction = X_processed.loc[
                        [original_index], features_used_in_model
                    ]

                    # Predict price
                    predicted_price = model.predict(property_features_for_prediction)[0]
                    actual_price = selected_property_original["SalePrice"]

                    st.header(f"Analysis for Property ID: {selected_id}")

                    col1, col2 = st.columns(2)

                    with col1:
                        st.subheader("Property Details")
                        # Display a subset of relevant features for the selected property
                        display_features = features_used_in_model + [
                            "SalePrice"
                        ]  # Show features used and actual price
                        details_to_show = selected_property_original[
                            display_features
                        ].copy()  # Make a copy
                        details_to_show.rename(
                            lambda x: x + " (Actual)", inplace=True
                        )  # Clarify these are actual values
                        st.table(
                            details_to_show.astype(str)
                        )  # Convert to string for consistent display

                    with col2:
                        st.subheader("Price Estimation")
                        st.metric(
                            label="Actual Sale Price", value=f"${actual_price:,.0f}"
                        )
                        st.metric(
                            label="Model's Estimated Price",
                            value=f"${predicted_price:,.0f}",
                        )

                        price_difference = actual_price - predicted_price
                        percentage_difference = (
                            (price_difference / predicted_price) * 100
                            if predicted_price
                            else 0
                        )

                        if (
                            abs(percentage_difference) < 1.0
                        ):  # Consider it "at market" if within +/- 1%
                            st.info(
                                f"💰 This property is priced **at** the estimated market value."
                            )
                        elif price_difference > 0:
                            st.warning(
                                f"📈 This property is priced **above** the estimated market value by ${price_difference:,.0f} ({percentage_difference:.2f}%)."
                            )
                        else:
                            st.success(
                                f"📉 This property is priced **below** the estimated market value by ${abs(price_difference):,.0f} ({abs(percentage_difference):.2f}%)."
                            )

                        st.markdown(
                            f"*(Based on a model trained on features: {', '.join(features_used_in_model)})*"
                        )
                else:
                    st.warning(
                        f"Could not find processed data for Property ID {selected_id}. This might happen if it had a missing 'SalePrice' and was excluded from training."
                    )
            else:
                st.info("Select a Property ID from the sidebar to see its analysis.")
        else:
            st.error(
                "Model could not be trained. Please check your data and feature selection."
            )
    else:
        st.markdown("---")  # Separator


if __name__ == "__main__":
    run_app()
