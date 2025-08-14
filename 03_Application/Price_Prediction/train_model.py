import pandas as pd
from sklearn.ensemble import RandomForestRegressor
from sklearn.impute import SimpleImputer
import numpy as np
import joblib  # For saving the model
import json  # For saving the list of features
import re  # For cleaning column names


# --- Utility to clean column names ---
def clean_col_names(df):
    """Cleans DataFrame column names: lowercase, replaces spaces/special chars with underscore."""
    new_cols = {}
    for col in df.columns:
        new_col = col.lower()  # Lowercase
        new_col = re.sub(r"\s+", "_", new_col)  # Replace spaces with underscore
        new_col = re.sub(
            r"[^0-9a-zA-Z_]", "", new_col
        )  # Remove special characters except underscore
        new_cols[col] = new_col
    df = df.rename(columns=new_cols)
    return df


# --- 1. Data Loading ---
def load_data(file_path):
    """Loads property data from a CSV file and cleans column names."""
    try:
        df = pd.read_csv(file_path)
        print(f"Data loaded successfully from '{file_path}'. Shape: {df.shape}")
        df = clean_col_names(df)  # Clean column names
        print(f"Cleaned column names: {df.columns.tolist()}")
        return df
    except FileNotFoundError:
        print(f"Error: The file '{file_path}' was not found.")
        return None
    except Exception as e:
        print(f"Error loading data: {e}")
        return None


# --- 2. Model Training ---
def train_price_model(df):
    """
    Trains a RandomForestRegressor model to predict the log of 'price'
    using selected numerical features, 'property_type', and 'neighborhood'.
    """
    if df is None:
        print("DataFrame is None. Cannot train model.")
        return None, None

    target_variable = "price"
    if target_variable not in df.columns:
        print(
            f"Error: Target variable '{target_variable}' column not found. Cannot train model."
        )
        return None, None

    # --- FEATURE SELECTION ---
    # Define the specific numerical and categorical features to use
    numerical_features_to_use = [
        "sq_m_built",
        "n_bedrooms",
        "bathrooms",
        "year_built",
        "floor",
        "latitude",
        "longitude",
        "n_photos",
        "sq_m_useful",
        # Add or remove numerical features as needed
    ]
    categorical_features_to_use = [
        "property_type",
        "neighborhood",
        # Add 'city' or 'district' if desired, but be mindful of cardinality
    ]

    # Filter to only features present in the dataframe
    available_numerical_features = [
        f for f in numerical_features_to_use if f in df.columns
    ]
    available_categorical_features = [
        f for f in categorical_features_to_use if f in df.columns
    ]

    if not available_numerical_features and not available_categorical_features:
        print(
            "Error: None of the selected numerical or categorical features are present in the data. Cannot train model."
        )
        return None, None

    print(f"Using numerical features: {available_numerical_features}")
    print(
        f"Using categorical features for one-hot encoding: {available_categorical_features}"
    )

    all_selected_features = (
        available_numerical_features + available_categorical_features
    )
    X = df[all_selected_features].copy()
    y_raw = df[target_variable].copy()

    # --- TARGET TRANSFORMATION ---
    y = np.log1p(y_raw)
    print(f"Target variable '{target_variable}' transformed using np.log1p().")
    # --- END TARGET TRANSFORMATION ---

    if y.isnull().any():
        missing_count = y.isnull().sum()
        print(
            f"Warning: {missing_count} rows with missing or invalid transformed '{target_variable}' will be dropped."
        )
        valid_indices = y.notnull()
        X = X.loc[valid_indices].copy()
        y = y.loc[valid_indices].copy()

    if X.empty or y.empty:
        print(
            f"Error: Not enough data to train after handling missing {target_variable} values."
        )
        return None, None

    processed_features_list = []

    # Process Numerical Features
    if available_numerical_features:
        X_numerical = X[available_numerical_features].copy()
        if "floor" in X_numerical.columns:
            X_numerical.loc[:, "floor"] = pd.to_numeric(
                X_numerical["floor"], errors="coerce"
            )

        num_imputer = SimpleImputer(strategy="median")
        X_numerical_imputed = num_imputer.fit_transform(X_numerical)
        X_numerical_processed = pd.DataFrame(
            X_numerical_imputed, columns=available_numerical_features, index=X.index
        )
        processed_features_list.append(X_numerical_processed)
        print(f"Processed numerical features: {X_numerical_processed.columns.tolist()}")

    # Process Categorical Features (One-Hot Encoding)
    if available_categorical_features:
        X_categorical = X[available_categorical_features].copy()
        for col in X_categorical.columns:  # Fill NaN before get_dummies
            X_categorical.loc[:, col] = X_categorical[col].fillna(
                "Missing_Category_Value"
            )  # Use a distinct fill value
            # Optional: Limit cardinality if a feature has too many unique values
            # top_n = 50 # Example: keep top 50 most frequent, group others
            # if X_categorical[col].nunique() > top_n:
            #     top_categories = X_categorical[col].value_counts().nlargest(top_n).index
            #     X_categorical.loc[~X_categorical[col].isin(top_categories), col] = 'Other_Category'

        try:
            X_categorical_encoded = pd.get_dummies(
                X_categorical, prefix=available_categorical_features, dummy_na=False
            )  # dummy_na=False as we filled NaNs
            processed_features_list.append(X_categorical_encoded)
            print(
                f"Processed categorical features (one-hot encoded): {X_categorical_encoded.columns.tolist()}"
            )
        except Exception as e:
            print(f"Error during one-hot encoding: {e}")
            # If one-hot encoding fails, you might not want to proceed or handle it differently
            return None, None

    if not processed_features_list:
        print(
            "Error: No features were processed (numerical or categorical). Cannot train model."
        )
        return None, None

    X_final_processed = pd.concat(processed_features_list, axis=1)
    # Ensure no duplicated columns if a numerical feature had same name as a categorical prefix (unlikely here)
    X_final_processed = X_final_processed.loc[
        :, ~X_final_processed.columns.duplicated()
    ]
    final_feature_names = X_final_processed.columns.tolist()

    if X_final_processed.empty:
        print("Error: Final processed feature set is empty. Cannot train model.")
        return None, None

    print(
        f"Final features for training ({len(final_feature_names)}): {final_feature_names}"
    )

    # Check for any remaining NaNs after all processing (should be handled by imputers)
    if X_final_processed.isnull().values.any():
        print(
            "Warning: NaN values detected in the final processed features. Imputing with median (overall)."
        )
        # This SimpleImputer will impute based on all columns.
        # It's a fallback; ideally, NaNs are handled per feature type or column.
        final_imputer = SimpleImputer(strategy="median")
        X_final_imputed_again = final_imputer.fit_transform(X_final_processed)
        X_final_processed = pd.DataFrame(
            X_final_imputed_again,
            columns=final_feature_names,
            index=X_final_processed.index,
        )

    # Using slightly adjusted hyperparameters as a starting point for RandomForest
    model = RandomForestRegressor(
        n_estimators=150,  # Increased slightly
        random_state=42,
        n_jobs=-1,
        max_depth=25,  # Increased slightly
        min_samples_split=10,  # Require more samples to split a node
        min_samples_leaf=5,  # Require more samples at a leaf node
        max_features="sqrt",  # Consider a subset of features at each split
    )
    try:
        model.fit(X_final_processed, y)
        print(
            "RandomForestRegressor model training completed successfully (target was log-transformed, simplified features)."
        )
    except Exception as e:
        print(f"Error during RandomForestRegressor model training: {e}")
        return None, None

    # --- Display Feature Importances ---
    if hasattr(model, "feature_importances_"):
        importances = model.feature_importances_
        # feature_names are already defined as final_feature_names
        importance_df = pd.DataFrame(
            {"feature": final_feature_names, "importance": importances}
        )
        importance_df = importance_df.sort_values(by="importance", ascending=False)
        print("\n--- Top 20 Feature Importances (Simplified Model) ---")
        print(importance_df.head(20))
        print("-----------------------------------------------------\n")
    # --- End Display Feature Importances ---

    return model, final_feature_names


# --- 3. Main Execution ---
if __name__ == "__main__":
    data_file_path = "Model_data_train.csv"
    # IMPORTANT: Save model with a name indicating it uses simplified features
    model_output_path = "property_price_model_rf_log_target_simple.joblib"
    features_output_path = "model_features_rf_log_target_simple.json"

    print(
        "--- Starting Simplified RandomForest Model Training Process (Log-Transformed Target) ---"
    )

    property_data = load_data(data_file_path)

    if property_data is not None:
        if (property_data["price"] < 0).any():
            print(
                "Error: Negative values found in 'price' column. Cannot apply log1p. Please clean data."
            )
            exit()  # or handle negative prices appropriately

        trained_model, features_used = train_price_model(property_data.copy())

        if trained_model and features_used:
            try:
                joblib.dump(trained_model, model_output_path)
                print(f"Model saved successfully to '{model_output_path}'")
            except Exception as e:
                print(f"Error saving model: {e}")

            try:
                with open(features_output_path, "w") as f:
                    json.dump(features_used, f)
                print(f"Model features saved successfully to '{features_output_path}'")
            except Exception as e:
                print(f"Error saving features: {e}")
        else:
            print(
                "Model training failed or no features available. Model and features not saved."
            )
    else:
        print("Data loading failed. Model training aborted.")

    print("--- Simplified RandomForest Model Training Process Finished ---")
