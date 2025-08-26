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
from modules.data_loaders import load_property_data_from_s3, load_prediction_model_and_features_from_s3
from modules.utils import *
from modules.config import (
    AWS_ACCESS_KEY_ID,
    AWS_SECRET_ACCESS_KEY,
    AWS_REGION,
    S3_MODEL_BUCKET_NAME,
    S3_DATA_BUCKET_NAME,
    MODEL_S3_KEY,
    FEATURES_S3_KEY,
    PROPERTY_DATA_S3_KEY,
)


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

def render():

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
            AWS_REGION,  # Pass credentials
        )

    prediction_model, model_training_features = load_prediction_model_and_features_from_s3(
        S3_MODEL_BUCKET_NAME,
        MODEL_S3_KEY,
        FEATURES_S3_KEY,
        AWS_ACCESS_KEY_ID,
        AWS_SECRET_ACCESS_KEY,
        AWS_REGION,  # Pass credentials
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
        if investment_summary and "error" in investment_summary:
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
                    format_currency(investment_summary["Total Initial Cost"]),
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
                            label=f"📥 Download {st.session_state.view_option} Data as CSV",
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
