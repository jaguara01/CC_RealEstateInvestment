# modules/utils.py

import re
import pandas as pd
import numpy as np


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


def clean_col_names(df):
    new_cols = {}
    for col in df.columns:
        new_col = col.lower()
        new_col = re.sub(r"\s+", "_", new_col)
        new_col = re.sub(r"[^0-9a-zA-Z_]", "", new_col)
        new_cols[col] = new_col
    df = df.rename(columns=new_cols)
    return df


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
        "Total Initial Cost": total_initial_cost,
    }
    return yearly_data, monthly_data, investment_summary, breakeven_month
