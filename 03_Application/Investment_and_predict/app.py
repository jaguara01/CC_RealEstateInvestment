import streamlit as st
import numpy as np
import pandas as pd
import joblib
import json
import re
import os
import boto3
import s3fs
import io

# --- Configuration: Model and Feature Files ---
# S3 Bucket Names
S3_MODEL_BUCKET_NAME = "07-model"
S3_DATA_BUCKET_NAME = "01-structured"

# Define paths for S3 objects
MODEL_S3_KEY = "property_price_model_rf_log_target_simple.joblib"
FEATURES_S3_KEY = "model_features_rf_log_target_simple.json"
PROPERTY_DATA_S3_KEY = "property-listings/idealista/Model_data_train.csv"

# --- AWS Credentials (TEMPORARY & INSECURE for PROOF OF CONCEPT ONLY) ---
# DO NOT USE IN PRODUCTION! Replace with your actual credentials.
AWS_ACCESS_KEY_ID = "AKIAQD5IIO4OIULVIK55"  # Replace with your Access Key ID
AWS_SECRET_ACCESS_KEY = (
    "7H4k+IM/2WSo32tfRqVZr5FOwTc+unQHnT0saLrl"  # Replace with your Secret Access Key
)
AWS_REGION_NAME = (
    "eu-west-1"  # Replace with your AWS Region (e.g., us-east-1, eu-central-1, etc.)
)
# --- END OF TEMPORARY & INSECURE CREDENTIALS SECTION ---


# Define the features exactly as used in the 'model_training_script_log_target_simple.py'
NUMERICAL_FEATURES_FOR_MODEL = [
    "sq_m_built",
    "n_bedrooms",
    "bathrooms",
    "year_built",
    "floor",
    "latitude",
    "longitude",
    "n_photos",
    "sq_m_useful",
]
CATEGORICAL_FEATURES_FOR_MODEL = ["property_type", "neighborhood"]

# Features to display in the UI for a selected property
DISPLAY_FEATURES = [
    "property_type",
    "sq_m_built",
    "n_bedrooms",
    "bathrooms",
    "year_built",
    "floor",
    "neighborhood",
    "n_photos",
    "sq_m_useful",
    "price",
]


# --- Helper Function: Clean Column Names ---
def clean_col_names(df):
    """Cleans DataFrame column names: lowercase, replaces spaces/special chars with underscore."""
    new_cols = {}
    for col in df.columns:
        new_col = col.lower()
        new_col = re.sub(r"\s+", "_", new_col)
        new_col = re.sub(r"[^0-9a-zA-Z_]", "", new_col)
        new_cols[col] = new_col
    df = df.rename(columns=new_cols)
    return df


# --- Helper Function: Load Property Data from S3 ---
@st.cache_data
def load_property_data_from_s3(
    bucket_name, s3_key, aws_access_key_id, aws_secret_access_key, aws_region_name
):
    """Loads property data from an S3 CSV file."""
    s3_path = f"s3://{bucket_name}/{s3_key}"
    try:
        # Pass credentials explicitly to s3fs.S3FileSystem
        fs = s3fs.S3FileSystem(
            key=aws_access_key_id,
            secret=aws_secret_access_key,
            client_kwargs={"region_name": aws_region_name},
        )
        with fs.open(s3_path, "rb") as f:
            df = pd.read_csv(f)  # Read from the opened file-like object

        df = clean_col_names(df)
        if "id" not in df.columns:
            st.error(f"Loaded CSV ('{s3_key}') must contain an 'id' column.")
            return None
        if "price" not in df.columns:
            st.error(f"Loaded CSV ('{s3_key}') must contain a 'price' column.")
            return None
        df["id"] = df["id"].astype(str)
        st.success(f"Property data loaded successfully from S3: '{s3_key}'.")
        return df
    except FileNotFoundError:
        st.error(
            f"Error: S3 object '{s3_key}' not found in bucket '{bucket_name}'. Please ensure the bucket name and key are correct and permissions are granted."
        )
        return None
    except Exception as e:
        st.error(f"Error loading or processing CSV from S3 '{s3_key}': {e}")
        return None


# --- Helper Function: Load Prediction Model and Training Features from S3 ---
@st.cache_resource
def load_prediction_model_and_features_from_s3(
    bucket_name,
    model_s3_key,
    features_s3_key,
    aws_access_key_id,
    aws_secret_access_key,
    aws_region_name,
):
    """Loads the trained model and the list of feature names from S3."""
    try:
        # Initialize boto3 client with explicit credentials
        s3 = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region_name,
        )

        # Load model
        # model_obj = s3.get_object(Bucket=bucket_name, Key=model_s3_key)
        # model = joblib.load(model_obj["Body"])

        model_obj = s3.get_object(Bucket=bucket_name, Key=model_s3_key)
        model_bytes_stream = io.BytesIO(
            model_obj["Body"].read()
        )  # This line is the fix
        model = joblib.load(model_bytes_stream)

        # Load features
        features_obj = s3.get_object(Bucket=bucket_name, Key=features_s3_key)
        training_features_list = json.loads(features_obj["Body"].read().decode("utf-8"))

        st.success(
            f"Prediction model '{model_s3_key}' and features loaded successfully from S3."
        )
        return model, training_features_list
    except s3.exceptions.NoSuchKey:
        st.warning(
            f"Prediction model ('{model_s3_key}') or features file ('{features_s3_key}') not found in S3 bucket '{bucket_name}'. Price prediction will be disabled. Please ensure the bucket name and key are correct and permissions are granted."
        )
        return None, None
    except Exception as e:
        st.error(f"Error loading prediction model or features from S3: {e}")
        return None, None


# --- Helper Function: Preprocess a Single Listing for Prediction ---
def preprocess_single_listing_for_prediction(
    _listing_series, _all_property_data_df, model_training_features_list
):
    """
    Preprocesses a single listing (Pandas Series) to match the training data format
    for the loaded model.
    """
    if _listing_series is None:
        return None
    listing_df = (
        _listing_series.to_frame().T.copy()
    )  # Ensure it's a DataFrame and a copy

    available_numerical = [
        f for f in NUMERICAL_FEATURES_FOR_MODEL if f in listing_df.columns
    ]
    available_categorical = [
        f for f in CATEGORICAL_FEATURES_FOR_MODEL if f in listing_df.columns
    ]
    processed_parts = []

    if available_numerical:
        num_df = listing_df[available_numerical].copy()
        if "floor" in num_df.columns:
            num_df.loc[:, "floor"] = pd.to_numeric(num_df["floor"], errors="coerce")
        for col in num_df.columns:
            if num_df[col].isnull().any():
                median_val = _all_property_data_df[
                    col
                ].median()  # Median from ENTIRE dataset
                num_df.loc[:, col] = num_df[col].fillna(median_val)
        processed_parts.append(num_df)

    if available_categorical:
        cat_df = listing_df[available_categorical].copy()
        for col in cat_df.columns:
            cat_df.loc[:, col] = cat_df[col].fillna("Missing_Category_Value")

        temp_full_cat_df = _all_property_data_df[available_categorical].copy()
        for col in temp_full_cat_df.columns:
            temp_full_cat_df.loc[:, col] = temp_full_cat_df[col].fillna(
                "Missing_Category_Value"
            )

        cat_df_encoded_single = pd.get_dummies(
            cat_df, prefix=available_categorical, dummy_na=False
        )
        all_possible_dummy_cols_df = pd.get_dummies(
            temp_full_cat_df, prefix=available_categorical, dummy_na=False
        )

        cat_df_reindexed = cat_df_encoded_single.reindex(
            columns=all_possible_dummy_cols_df.columns, fill_value=0
        )
        processed_parts.append(cat_df_reindexed)

    if not processed_parts:
        st.warning(
            "No features could be processed for price prediction for the selected listing."
        )
        return None

    combined_df = pd.concat(processed_parts, axis=1)
    final_prediction_input_df = pd.DataFrame(
        columns=model_training_features_list, index=combined_df.index
    ).fillna(0)

    for col in combined_df.columns:
        if col in final_prediction_input_df.columns:
            final_prediction_input_df.loc[:, col] = combined_df[col].values

    if final_prediction_input_df.isnull().values.any():
        st.warning("NaNs detected in final prediction input. Filling with 0.")
        final_prediction_input_df = final_prediction_input_df.fillna(0)

    return final_prediction_input_df[model_training_features_list]


# --- Core Investment Analysis Logic ---
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
    if property_value is None:
        property_value = property_price
    total_initial_cost = property_price + legal_buying_fees + initial_fees
    loan_amount = total_initial_cost - down_payment
    if loan_amount <= 0:
        return (
            None,
            None,
            {"error": "Down payment covers or exceeds total initial cost."},
            None,
        )
    if (
        interest_rate < 0
        or loan_term_years <= 0
        or property_price <= 0
        or down_payment <= 0
    ):
        return (
            None,
            None,
            {"error": "Loan term, property price, and down payment must be positive."},
            None,
        )
    monthly_interest_rate = interest_rate / 12
    num_payments = loan_term_years * 12
    if monthly_interest_rate > 0:
        try:
            monthly_payment = (
                loan_amount
                * (monthly_interest_rate * (1 + monthly_interest_rate) ** num_payments)
                / ((1 + monthly_interest_rate) ** num_payments - 1)
            )
        except OverflowError:
            return None, None, {"error": "Calculation resulted in overflow."}, None
        except ValueError:
            return None, None, {"error": "Calculation error (PMT)."}, None
    else:
        monthly_payment = loan_amount / num_payments if num_payments > 0 else 0
    monthly_expenses = monthly_rent * monthly_expenses_percent
    monthly_net_rental_income = monthly_rent - monthly_expenses
    remaining_capital = np.zeros(num_payments + 1)
    interest_paid = np.zeros(num_payments)
    principal_paid = np.zeros(num_payments)
    accumulated_amortization = np.zeros(num_payments + 1)
    property_value_over_time = np.zeros(num_payments + 1)
    accumulated_profit_if_sold = np.zeros(num_payments + 1)
    total_monthly_income_no_sale = np.zeros(num_payments)
    cumulative_income_no_sale = np.zeros(num_payments + 1)
    remaining_capital[0] = loan_amount
    property_value_over_time[0] = property_value
    accumulated_profit_if_sold[0] = (
        property_value_over_time[0] - remaining_capital[0] - down_payment
    )
    for i in range(num_payments):
        interest_paid_current_month = remaining_capital[i] * monthly_interest_rate
        if monthly_payment < interest_paid_current_month and monthly_interest_rate > 0:
            principal_paid_current_month = 0
        else:
            principal_paid_current_month = monthly_payment - interest_paid_current_month
        principal_paid_current_month = min(
            principal_paid_current_month, remaining_capital[i]
        )
        interest_paid[i] = interest_paid_current_month
        principal_paid[i] = principal_paid_current_month
        remaining_capital[i + 1] = remaining_capital[i] - principal_paid[i]
        remaining_capital[i + 1] = max(0, remaining_capital[i + 1])
        accumulated_amortization[i + 1] = (
            accumulated_amortization[i] + principal_paid[i]
        )
        monthly_appreciation_rate = (
            (1 + annual_appreciation_rate) ** (1 / 12) - 1
            if annual_appreciation_rate != 0
            else 0
        )
        property_value_over_time[i + 1] = property_value_over_time[i] * (
            1 + monthly_appreciation_rate
        )
        total_monthly_income_no_sale[i] = principal_paid[i] + monthly_net_rental_income
        cumulative_income_no_sale[i + 1] = (
            cumulative_income_no_sale[i] + total_monthly_income_no_sale[i]
        )
        accumulated_profit_if_sold[i + 1] = (
            property_value_over_time[i + 1]
            - remaining_capital[i + 1]
            + ((i + 1) * monthly_net_rental_income)
            - down_payment
        )
    monthly_data = pd.DataFrame(
        {
            "Month": range(1, num_payments + 1),
            "Remaining_Capital": remaining_capital[1:],
            "Interest_Paid": interest_paid,
            "Principal_Paid": principal_paid,
            "Monthly_Payment": monthly_payment,
            "Monthly_Net_Rental_Income": monthly_net_rental_income,
            "Total_Monthly_Income_No_Sale": total_monthly_income_no_sale,
            "Accumulated_Amortization": accumulated_amortization[1:],
            "Property_Value": property_value_over_time[1:],
            "Cumulative_Income_No_Sale": cumulative_income_no_sale[1:],
            "Accumulated_Profit_If_Sold": accumulated_profit_if_sold[1:],
        }
    )
    monthly_data["Year"] = (monthly_data["Month"] - 1) // 12 + 1
    breakeven_month = None
    breakeven_indices = np.where(monthly_data["Accumulated_Profit_If_Sold"] >= 0)[0]
    if len(breakeven_indices) > 0:
        first_breakeven_index = breakeven_indices[0]
        if first_breakeven_index < len(monthly_data):
            breakeven_month = monthly_data["Month"].iloc[first_breakeven_index]
    yearly_data = (
        monthly_data.groupby("Year")
        .agg(
            Remaining_Capital=("Remaining_Capital", "last"),
            Interest_Paid=("Interest_Paid", "sum"),
            Principal_Paid=("Principal_Paid", "sum"),
            Average_Monthly_Payment=("Monthly_Payment", "mean"),
            Annual_Net_Rental_Income=("Monthly_Net_Rental_Income", "sum"),
            Annual_Income_No_Sale=("Total_Monthly_Income_No_Sale", "sum"),
            Accumulated_Amortization=("Accumulated_Amortization", "last"),
            Property_Value=("Property_Value", "last"),
            Cumulative_Income_No_Sale=("Cumulative_Income_No_Sale", "last"),
            Accumulated_Profit_If_Sold=("Accumulated_Profit_If_Sold", "last"),
        )
        .reset_index()
    )
    yearly_data["Total_Equity"] = (
        yearly_data["Property_Value"] - yearly_data["Remaining_Capital"]
    )
    if down_payment > 0:
        yearly_data["ROI_If_Sold"] = (
            yearly_data["Accumulated_Profit_If_Sold"] / down_payment * 100
        )
        yearly_data["ROI_No_Sale"] = (
            yearly_data["Cumulative_Income_No_Sale"] / down_payment * 100
        )
    else:
        yearly_data["ROI_If_Sold"] = np.nan
        yearly_data["ROI_No_Sale"] = np.nan
    years = yearly_data["Year"].to_numpy()
    roi_sold_base = 1 + yearly_data["ROI_If_Sold"].to_numpy() / 100
    roi_no_sale_base = 1 + yearly_data["ROI_No_Sale"].to_numpy() / 100
    safe_roi_sold_base = np.maximum(0.00001, roi_sold_base)
    safe_roi_no_sale_base = np.maximum(0.00001, roi_no_sale_base)
    years_safe = np.maximum(1, years)
    yearly_data["Annualized_ROI_If_Sold"] = (
        np.power(safe_roi_sold_base, 1 / years_safe) - 1
    ) * 100
    yearly_data["Annualized_ROI_No_Sale"] = (
        np.power(safe_roi_no_sale_base, 1 / years_safe) - 1
    ) * 100
    investment_summary = {
        "Property Purchase Price": property_price,
        "Estimated Initial Property Value": property_value,
        "Legal & Buying Fees": legal_buying_fees,
        "Other Initial Fees": initial_fees,
        "Down Payment (Initial Cash Investment)": down_payment,
        "Loan Amount": loan_amount,
        "Loan Term (Years)": loan_term_years,
        "Annual Interest Rate (%)": interest_rate * 100,
        "Monthly Loan Payment": monthly_payment,
        "Monthly Rent": monthly_rent,
        "Monthly Expenses (%)": monthly_expenses_percent * 100,
        "Monthly Expenses (€)": monthly_expenses,
        "Monthly Net Rental Income": monthly_net_rental_income,
        "Annual Appreciation Rate (%)": annual_appreciation_rate * 100,
    }
    return yearly_data, monthly_data, investment_summary, breakeven_month


# --- Helper Function for Formatting ---
def format_currency(value):
    if pd.isna(value) or value is None:
        return "N/A"
    if np.isinf(value):
        return "Infinity"
    return f"€{value:,.0f}"


def format_percentage(value):
    if pd.isna(value) or value is None:
        return "N/A"
    if np.isinf(value):
        return "Infinity"
    return f"{value:.1f}%"


def format_feature_name(feature_name):
    """Formats a snake_case feature name into a more readable title."""
    return feature_name.replace("_", " ").title().replace("Sq M", "Sq. M.")


# --- Streamlit App UI ---
st.set_page_config(
    layout="wide", page_title="Real Estate Investment Analyzer with Prediction"
)
st.title("🏡 Real Estate Investment Analyzer & Price Check")

# --- Initialize Session State ---
if "analysis_results" not in st.session_state:
    st.session_state.analysis_results = None
if "view_option" not in st.session_state:
    st.session_state.view_option = "Annual"
if "property_data_df" not in st.session_state:
    st.session_state.property_data_df = None
if "selected_property_id" not in st.session_state:
    st.session_state.selected_property_id = None
if "predicted_price_info" not in st.session_state:
    st.session_state.predicted_price_info = None
if "property_price_input" not in st.session_state:
    st.session_state.property_price_input = 279000
if "selected_property_details" not in st.session_state:
    st.session_state.selected_property_details = None

# --- Load Data and Model at Startup ---
if st.session_state.property_data_df is None:
    st.session_state.property_data_df = load_property_data_from_s3(
        S3_DATA_BUCKET_NAME,
        PROPERTY_DATA_S3_KEY,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION_NAME,  # Pass credentials
    )

prediction_model, model_training_features = load_prediction_model_and_features_from_s3(
    S3_MODEL_BUCKET_NAME,
    MODEL_S3_KEY,
    FEATURES_S3_KEY,
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION_NAME,  # Pass credentials
)

# --- Sidebar ---
st.sidebar.header("Property & Price (€)")

# Property Selection Dropdown
if st.session_state.property_data_df is not None:
    property_ids = ["Enter Manually"] + sorted(
        st.session_state.property_data_df["id"].unique().tolist()
    )

    if st.session_state.selected_property_id in property_ids:
        current_selection_index = property_ids.index(
            st.session_state.selected_property_id
        )
    else:
        current_selection_index = 0
        st.session_state.selected_property_id = "Enter Manually"

    selected_id_on_sidebar = st.sidebar.selectbox(
        "Select Property ID or Enter Manually:",
        options=property_ids,
        index=current_selection_index,
        key="prop_id_selector",
    )

    if selected_id_on_sidebar != st.session_state.selected_property_id:
        st.session_state.selected_property_id = selected_id_on_sidebar
        if st.session_state.selected_property_id != "Enter Manually":
            selected_property_details_new = st.session_state.property_data_df[
                st.session_state.property_data_df["id"]
                == st.session_state.selected_property_id
            ].iloc[0]
            st.session_state.property_price_input = int(
                selected_property_details_new["price"]
            )
            st.session_state.selected_property_details = selected_property_details_new
            st.session_state.predicted_price_info = None

            if prediction_model and model_training_features:
                processed_listing = preprocess_single_listing_for_prediction(
                    selected_property_details_new,
                    st.session_state.property_data_df,
                    model_training_features,
                )
                if processed_listing is not None and not processed_listing.empty:
                    try:
                        pred_log_price = prediction_model.predict(processed_listing)[0]
                        pred_price = np.expm1(pred_log_price)
                        st.session_state.predicted_price_info = {
                            "original": selected_property_details_new["price"],
                            "predicted": pred_price,
                        }
                    except Exception as e:
                        st.sidebar.error(f"Price prediction error: {e}")
        else:
            st.session_state.predicted_price_info = None
            st.session_state.selected_property_details = None
        st.rerun()

elif st.session_state.property_data_df is None:
    st.sidebar.warning(
        f"Could not load data from S3. Property selection and prediction disabled."
    )
    st.session_state.selected_property_id = "Enter Manually"

property_price_from_input_widget = st.sidebar.number_input(
    "Property Purchase Price (€)",
    min_value=1,
    value=st.session_state.property_price_input,
    step=1000,
    key="property_price_man_input_widget",
    disabled=(
        st.session_state.selected_property_id != "Enter Manually"
        and st.session_state.property_data_df is not None
    ),
)

if st.session_state.selected_property_id == "Enter Manually":
    if st.session_state.property_price_input != property_price_from_input_widget:
        st.session_state.property_price_input = property_price_from_input_widget
        st.rerun()

property_value_option = st.sidebar.radio(
    "Initial Property Value Source",
    ["Same as Purchase Price", "Estimate Manually"],
    index=0,
    key="prop_val_opt",
)
if property_value_option == "Estimate Manually":
    property_value_in = st.sidebar.number_input(
        "Estimated Initial Value (€)",
        min_value=0,
        value=st.session_state.property_price_input,
        step=1000,
        key="prop_val_man",
    )
else:
    property_value_in = None

legal_fees_percent = st.sidebar.slider(
    "Legal & Buying Fees (% of Price)",
    0.0,
    30.0,
    10.0,
    0.1,
    format="%.1f%%",
    key="legal_fees",
)
legal_buying_fees = st.session_state.property_price_input * (legal_fees_percent / 100.0)
initial_fees = st.sidebar.number_input(
    "Other Initial Fees (€)", min_value=0, value=1000, step=100, key="init_fees"
)

st.sidebar.markdown("---")
st.sidebar.header("Loan & Financing (€)")
total_initial_cost_ref = (
    st.session_state.property_price_input + legal_buying_fees + initial_fees
)
st.sidebar.markdown(
    f"<small>Total Initial Cost: {format_currency(total_initial_cost_ref)}</small>",
    unsafe_allow_html=True,
)

down_payment_option = st.sidebar.radio(
    "Down Payment Input", ["Percentage", "Amount"], index=1, key="dp_opt"
)
if down_payment_option == "Percentage":
    down_payment_percent = st.sidebar.slider(
        "Down Payment (% of Total Cost)",
        0.1,
        100.0,
        20.0,
        0.5,
        format="%.1f%%",
        key="dp_perc",
    )
    down_payment = total_initial_cost_ref * (down_payment_percent / 100.0)
else:
    default_dp_val = int(max(1, total_initial_cost_ref * 0.2))
    down_payment = st.sidebar.number_input(
        "Down Payment Amount (€)",
        min_value=1,
        value=default_dp_val,
        step=100,
        key="dp_amt",
    )

st.sidebar.markdown(
    f"<small>Calculated Down Payment: {format_currency(down_payment)}</small>",
    unsafe_allow_html=True,
)
loan_amount_disp = max(0, total_initial_cost_ref - down_payment)
st.sidebar.markdown(
    f"<small>Resulting Loan Amount: {format_currency(loan_amount_disp)}</small>",
    unsafe_allow_html=True,
)

interest_rate_percent = st.sidebar.slider(
    "Annual Interest Rate (%)", 0.0, 15.0, 3.0, 0.05, format="%.2f%%", key="int_rate"
)
interest_rate = interest_rate_percent / 100.0
loan_term_years = st.sidebar.slider("Loan Term (Years)", 1, 40, 25, 1, key="loan_term")

st.sidebar.markdown("---")
st.sidebar.header("Income & Growth (€)")
monthly_rent = st.sidebar.number_input(
    "Monthly Rental Income (€)", min_value=0, value=1000, step=25, key="rent"
)
monthly_expenses_percent_in = st.sidebar.slider(
    "Monthly Expenses (% of Rent)",
    0.0,
    100.0,
    20.0,
    1.0,
    format="%.0f%%",
    key="exp_perc",
)
monthly_expenses_percent = monthly_expenses_percent_in / 100.0
annual_appreciation_rate_percent = st.sidebar.slider(
    "Annual Property Appreciation Rate (%)",
    -5.0,
    15.0,
    3.0,
    0.1,
    format="%.1f%%",
    key="appr_rate",
)
annual_appreciation_rate = annual_appreciation_rate_percent / 100.0

# --- Analyze Button Logic ---
if st.sidebar.button("Analyze Investment", key="analyze_btn", type="primary"):
    results = real_estate_investment_analysis(
        property_value=property_value_in,
        property_price=st.session_state.property_price_input,
        legal_buying_fees=legal_buying_fees,
        initial_fees=initial_fees,
        down_payment=down_payment,
        interest_rate=interest_rate,
        loan_term_years=loan_term_years,
        annual_appreciation_rate=annual_appreciation_rate,
        monthly_rent=monthly_rent,
        monthly_expenses_percent=monthly_expenses_percent,
    )
    st.session_state.analysis_results = results

# --- Main Display Area ---
if st.session_state.analysis_results:
    yearly_data, monthly_data, investment_summary, breakeven_month = (
        st.session_state.analysis_results
    )
    if isinstance(investment_summary, dict) and "error" in investment_summary:
        st.error(f"Analysis Error: {investment_summary['error']}")
    elif (
        yearly_data is not None
        and investment_summary is not None
        and monthly_data is not None
    ):
        st.header("Investment Summary")

        # Display Property Features if a property is selected
        if st.session_state.selected_property_details is not None:
            st.subheader(
                f"Property Features (ID: {st.session_state.selected_property_id})"
            )
            features_to_show = {}
            for feature in DISPLAY_FEATURES:
                if feature in st.session_state.selected_property_details.index:
                    features_to_show[format_feature_name(feature)] = (
                        st.session_state.selected_property_details[feature]
                    )

            features_df = pd.DataFrame(
                features_to_show.items(), columns=["Feature", "Value"]
            )
            st.table(features_df)
            st.markdown("---")

        if st.session_state.predicted_price_info:  # Display Price Prediction
            pred_info = st.session_state.predicted_price_info
            original_p = pred_info["original"]
            predicted_p = pred_info["predicted"]
            diff = original_p - predicted_p
            percentage_diff = (
                (diff / predicted_p * 100) if predicted_p and predicted_p != 0 else 0
            )
            pred_col1, pred_col2, pred_col3 = st.columns(3)
            with pred_col1:
                st.metric("Actual Listing Price", format_currency(original_p))
            with pred_col2:
                st.metric("Model Predicted Price", format_currency(predicted_p))
            with pred_col3:
                if abs(percentage_diff) < 2.0:
                    st.metric(
                        "Market Comparison",
                        "At Market Value",
                        help=f"{percentage_diff:.2f}% diff",
                    )
                elif diff > 0:
                    st.metric(
                        "Market Comparison",
                        "Above Market",
                        delta=f"{percentage_diff:.1f}%",
                        delta_color="inverse",
                        help=f"Priced {format_currency(abs(diff))} above model.",
                    )
                else:
                    st.metric(
                        "Market Comparison",
                        "Below Market",
                        delta=f"{percentage_diff:.1f}%",
                        delta_color="normal",
                        help=f"Priced {format_currency(abs(diff))} below model.",
                    )
            st.markdown("---")
        col_s1, col_s2, col_s3, col_s4 = st.columns(4)  # Summary Metrics
        with col_s1:
            st.metric(
                "Initial Investment",
                format_currency(
                    investment_summary["Down Payment (Initial Cash Investment)"]
                ),
            )
            st.metric(
                "Property Price",
                format_currency(investment_summary["Property Purchase Price"]),
            )
            st.metric("Loan Amount", format_currency(investment_summary["Loan Amount"]))
        with col_s2:
            st.metric(
                "Monthly Payment",
                format_currency(investment_summary["Monthly Loan Payment"]),
            )
            st.metric(
                "Monthly Net Rent",
                format_currency(investment_summary["Monthly Net Rental Income"]),
            )
            st.metric(
                "Interest Rate",
                f"{investment_summary['Annual Interest Rate (%)']:.2f}%",
            )
        with col_s3:
            st.metric(
                "Appreciation Rate",
                f"{investment_summary['Annual Appreciation Rate (%)']:.1f}%",
            )
            st.metric("Loan Term", f"{investment_summary['Loan Term (Years)']} Years")
            st.metric(
                "Total Initial Cost",
                format_currency(total_initial_cost_ref),
            )
        with col_s4:
            if breakeven_month is not None:
                st.metric("Profit Breakeven Month", f"Month {int(breakeven_month)}")
            else:
                st.metric("Profit Breakeven Month", "Not Reached")
        st.markdown("---")
        st.header("Projections")
        st.session_state.view_option = st.radio(
            "Select View:",
            ("Annual", "Monthly"),
            index=("Annual", "Monthly").index(st.session_state.view_option),
            key="view_select_radio",
            horizontal=True,
        )

        if st.session_state.view_option == "Annual":
            if yearly_data is None or yearly_data.empty:
                plot_data, table_data = None, None
                st.warning("Annual data not available.")
            else:
                plot_data = yearly_data.set_index("Year")
                table_data = yearly_data.copy()
                x_label, chart_title_prefix = "Year", "Annual"
                display_cols = [
                    "Year",
                    "Property_Value",
                    "Remaining_Capital",
                    "Total_Equity",
                    "Annual_Net_Rental_Income",
                    "Principal_Paid",
                    "Interest_Paid",
                    "Accumulated_Profit_If_Sold",
                    "ROI_If_Sold",
                    "ROI_No_Sale",
                    "Annualized_ROI_If_Sold",
                    "Annualized_ROI_No_Sale",
                ]
                currency_cols = [
                    "Property_Value",
                    "Remaining_Capital",
                    "Total_Equity",
                    "Annual_Net_Rental_Income",
                    "Principal_Paid",
                    "Interest_Paid",
                    "Accumulated_Profit_If_Sold",
                ]
                percent_cols = [
                    "ROI_If_Sold",
                    "ROI_No_Sale",
                    "Annualized_ROI_If_Sold",
                    "Annualized_ROI_No_Sale",
                ]
        else:  # Monthly View
            if monthly_data is None or monthly_data.empty:
                plot_data, table_data = None, None
                st.warning("Monthly data not available.")
            else:
                if "Total_Equity" not in monthly_data.columns:
                    monthly_data["Total_Equity"] = (
                        monthly_data["Property_Value"]
                        - monthly_data["Remaining_Capital"]
                    )
                plot_data = monthly_data.set_index("Month")
                table_data = monthly_data.copy()
                x_label, chart_title_prefix = "Month", "Monthly"
                display_cols = [
                    "Month",
                    "Year",
                    "Property_Value",
                    "Remaining_Capital",
                    "Total_Equity",
                    "Monthly_Net_Rental_Income",
                    "Principal_Paid",
                    "Interest_Paid",
                    "Monthly_Payment",
                    "Accumulated_Profit_If_Sold",
                    "Cumulative_Income_No_Sale",
                ]
                currency_cols = [
                    "Property_Value",
                    "Remaining_Capital",
                    "Total_Equity",
                    "Monthly_Net_Rental_Income",
                    "Principal_Paid",
                    "Interest_Paid",
                    "Monthly_Payment",
                    "Accumulated_Profit_If_Sold",
                    "Cumulative_Income_No_Sale",
                ]
                percent_cols = []
        st.subheader(f"{chart_title_prefix} Visualizations")  # Charts
        if plot_data is not None and not plot_data.empty:
            chart_col1, chart_col2 = st.columns(2)
            with chart_col1:
                st.markdown(f"**Property Value vs. Loan ({x_label})**")
                st.line_chart(plot_data[["Property_Value", "Remaining_Capital"]])
                st.markdown(f"**Accumulated Profit/Income ({x_label})")
                st.line_chart(
                    plot_data[
                        ["Accumulated_Profit_If_Sold", "Cumulative_Income_No_Sale"]
                    ]
                )
                if st.session_state.view_option == "Annual":
                    st.markdown(f"**Annualized ROI ({x_label})**")
                    st.line_chart(
                        plot_data[["Annualized_ROI_If_Sold", "Annualized_ROI_No_Sale"]]
                    )
                else:
                    st.markdown(f"**Monthly Cash Flow (First 3 Years)**")
                    st.line_chart(
                        plot_data[
                            [
                                "Principal_Paid",
                                "Monthly_Net_Rental_Income",
                                "Total_Monthly_Income_No_Sale",
                            ]
                        ].head(min(36, len(plot_data)))
                    )
            with chart_col2:
                st.markdown(f"**Loan Payment Breakdown ({x_label})**")
                st.bar_chart(plot_data[["Interest_Paid", "Principal_Paid"]])
                if st.session_state.view_option == "Annual":
                    st.markdown(f"**Total ROI ({x_label})**")
                    st.line_chart(plot_data[["ROI_If_Sold", "ROI_No_Sale"]])
                st.markdown(f"**Total Equity Growth ({x_label})**")
                st.area_chart(plot_data["Total_Equity"])
        else:
            st.info("Run analysis to view charts.")
        st.subheader(f"{chart_title_prefix} Projections Data")  # Data Table
        if table_data is not None and not table_data.empty:
            valid_display_cols = [
                col for col in display_cols if col in table_data.columns
            ]
            if valid_display_cols:
                formatted_table_data = table_data[valid_display_cols].copy()
                for col in currency_cols:
                    if col in formatted_table_data.columns:
                        formatted_table_data[col] = formatted_table_data[col].apply(
                            format_currency
                        )
                for col in percent_cols:
                    if col in formatted_table_data.columns:
                        formatted_table_data[col] = formatted_table_data[col].apply(
                            format_percentage
                        )
                st.dataframe(
                    formatted_table_data, use_container_width=True, hide_index=True
                )

                @st.cache_data
                def convert_df_to_csv(df_to_convert, cols_to_convert):
                    if df_to_convert is None:
                        return None
                    valid_cols = [
                        col for col in cols_to_convert if col in df_to_convert.columns
                    ]
                    if not valid_cols:
                        return None
                    return df_to_convert[valid_cols].to_csv(index=False).encode("utf-8")

                csv_data = convert_df_to_csv(table_data, display_cols)
                if csv_data:
                    st.download_button(
                        label=f"Download {st.session_state.view_option} Data as CSV",
                        data=csv_data,
                        file_name=f"{st.session_state.view_option.lower()}_investment_projection.csv",
                        mime="text/csv",
                        key=f"dl_{st.session_state.view_option.lower()}",
                    )
            else:
                st.warning("No valid columns for data table.")
        else:
            st.info("Run analysis to view data table.")
    elif (
        st.session_state.analysis_results is not None
        and st.session_state.analysis_results[0] is None
        and (not investment_summary or "error" not in investment_summary)
    ):
        st.warning("Analysis could not be completed. Check inputs.")
elif not st.session_state.analysis_results:
    st.info(
        "Adjust parameters in the sidebar and click 'Analyze Investment' to see results."
    )
