import pandas as pd
import numpy as np
import joblib
import json
import re


# --- Utility to clean column names (same as training script) ---
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


# --- 1. Load Model, Features, and Data ---
def load_model_and_features(model_path, features_path):
    """Loads the trained model and the list of feature names."""
    try:
        model = joblib.load(model_path)
        with open(features_path, "r") as f:
            training_features = json.load(f)
        print(
            f"Model '{model_path}' and features '{features_path}' loaded successfully."
        )
        return model, training_features
    except FileNotFoundError:
        print(
            f"Error: Model or features file not found. Searched for '{model_path}' and '{features_path}'."
        )
        return None, None
    except Exception as e:
        print(f"Error loading model or features: {e}")
        return None, None


def load_full_data(file_path):
    """Loads the full property data CSV for context and preprocessing."""
    try:
        df = pd.read_csv(file_path)
        df = clean_col_names(df)  # Clean column names immediately
        if "id" in df.columns:
            try:
                df["id"] = df["id"].astype(
                    str
                )  # Ensure 'id' is string for consistent indexing
                df = df.set_index("id", drop=False)  # Keep id as a column too
                print(
                    f"Data loaded from '{file_path}'. Shape: {df.shape}. 'id' column set as index."
                )
            except Exception as e:
                print(
                    f"Warning: Could not set 'id' as index, or 'id' column has issues: {e}"
                )
        else:
            print(
                f"Data loaded from '{file_path}'. Shape: {df.shape}. 'id' column not found for indexing."
            )
        return df
    except FileNotFoundError:
        print(f"Error: Data file '{file_path}' was not found.")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


# --- 2. Preprocess a Single Listing for Prediction ---
def preprocess_single_listing(
    listing_series, all_training_data_df, final_feature_names_from_training
):
    """
    Preprocesses a single listing (Pandas Series) to match the training data format.
    """
    if listing_series is None:
        return None

    print(f"\nPreprocessing listing ID: {listing_series.get('id', 'N/A')}")
    listing_df = listing_series.to_frame().T

    original_numerical_cols = [
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
    original_categorical_cols = [
        "property_type",
        "city",
        "neighborhood",
        "district",
        "province",
        "country",
        "source",
        "operation",
        "property_subtype",
        "building_condition",
    ]
    original_boolean_cols = [
        "lift",
        "terrace",
        "balcony",
        "parking",
        "swimming_pool",
        "garden",
        "air_conditioning",
        "heating",
        "exterior",
        "second_hand",
        "needs_renovating",
    ]

    available_numerical = [
        col for col in original_numerical_cols if col in listing_df.columns
    ]
    available_categorical = [
        col for col in original_categorical_cols if col in listing_df.columns
    ]
    available_boolean = [
        col for col in original_boolean_cols if col in listing_df.columns
    ]

    processed_parts = []

    if available_numerical:
        num_df = listing_df[available_numerical].copy()
        if "floor" in num_df.columns:
            num_df.loc[:, "floor"] = pd.to_numeric(num_df["floor"], errors="coerce")
        for col in num_df.columns:
            if num_df[col].isnull().any():
                median_val = all_training_data_df[col].median()
                num_df.loc[:, col] = num_df[col].fillna(median_val)
        processed_parts.append(num_df)

    if available_boolean:
        bool_df = listing_df[available_boolean].copy()
        for col in bool_df.columns:
            bool_df.loc[:, col] = (
                bool_df[col]
                .replace(
                    {True: 1, "True": 1, "TRUE": 1, False: 0, "False": 0, "FALSE": 0}
                )
                .infer_objects(copy=False)
            )
            bool_df.loc[:, col] = (
                pd.to_numeric(bool_df[col], errors="coerce").fillna(0).astype(int)
            )
        processed_parts.append(bool_df)

    if available_categorical:
        cat_df = listing_df[available_categorical].copy()
        for col in cat_df.columns:
            cat_df.loc[:, col] = cat_df[col].fillna("Missing")
        cat_df_encoded = pd.get_dummies(cat_df, prefix=cat_df.columns, dummy_na=False)
        processed_parts.append(cat_df_encoded)

    if not processed_parts:
        print("Error: No features could be processed for the listing.")
        return None

    combined_df = pd.concat(processed_parts, axis=1)
    final_prediction_input_df = pd.DataFrame(
        columns=final_feature_names_from_training, index=combined_df.index
    ).fillna(0)

    for col in combined_df.columns:
        if col in final_prediction_input_df.columns:
            final_prediction_input_df.loc[:, col] = combined_df[
                col
            ].values  # Use .values to assign correctly for single row

    if final_prediction_input_df.isnull().values.any():
        print(
            "Warning: NaNs detected in final prediction input after alignment. Filling with 0."
        )
        # For debugging: print(final_prediction_input_df.isnull().sum()[final_prediction_input_df.isnull().sum() > 0])
        final_prediction_input_df = final_prediction_input_df.fillna(0)

    return final_prediction_input_df[final_feature_names_from_training]


# --- 3. Main Execution ---
if __name__ == "__main__":
    # --- IMPORTANT: Use the model and features from the log-target training ---
    model_file = "property_price_model_rf_log_target_simple.joblib"
    features_file = "model_features_rf_log_target_simple.json"
    data_file = "Model_data_train.csv"

    model, training_features_list = load_model_and_features(model_file, features_file)
    if model is None or training_features_list is None:
        print("Exiting due to loading errors.")
        exit()

    full_df = load_full_data(data_file)
    if full_df is None:
        print("Exiting due to data loading error.")
        exit()

    while True:
        listing_id_input = input(
            f"Enter Listing ID to predict (or type 'exit' to quit): "
        ).strip()
        if listing_id_input.lower() == "exit":
            break
        if not listing_id_input:
            print("Please enter an ID.")
            continue

        try:
            selected_listing_series = full_df.loc[str(listing_id_input)]
            if isinstance(selected_listing_series, pd.DataFrame):
                if len(selected_listing_series) > 0:
                    print(
                        f"Warning: Multiple listings found for ID '{listing_id_input}'. Using the first one."
                    )
                    selected_listing_series = selected_listing_series.iloc[0]
                else:
                    print(f"No listing found with ID '{listing_id_input}'.")
                    continue

            original_price = selected_listing_series.get("price", "N/A")

            processed_listing_df = preprocess_single_listing(
                selected_listing_series, full_df, training_features_list
            )

            if processed_listing_df is not None and not processed_listing_df.empty:
                # Make prediction (this will be log-transformed price)
                predicted_log_price = model.predict(processed_listing_df)[0]

                # --- INVERSE TRANSFORM THE PREDICTION ---
                # Convert the log-transformed prediction back to the original price scale
                predicted_price = np.expm1(predicted_log_price)
                # --- END INVERSE TRANSFORM ---

                print("\n--- Prediction Result ---")
                print(f"Listing ID:         {listing_id_input}")
                if original_price != "N/A":
                    print(f"Original Price:       €{original_price:,.2f}")
                else:
                    print("Original Price:       Not Available")
                print(
                    f"Predicted (log) Price: {predicted_log_price:.4f}"
                )  # Display the raw log prediction for debugging
                print(f"Predicted Price (actual): €{predicted_price:,.2f}")
                print("-------------------------\n")
            else:
                print(
                    f"Could not preprocess or predict for listing ID '{listing_id_input}'."
                )

        except KeyError:
            print(
                f"No listing found with ID '{listing_id_input}'. Please check the ID and CSV file ('{data_file}')."
            )
        except Exception as e:
            print(f"An error occurred: {e}")
            import traceback

            traceback.print_exc()  # Uncomment for detailed error trace

    print("Prediction script finished.")
