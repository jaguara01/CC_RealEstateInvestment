# modules/data_loaders.py

import os, json
import pandas as pd
import boto3
import s3fs
import io
import joblib
from functools import lru_cache
import streamlit as st
from .config import (
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION,
    LISTINGS_BUCKET, LISTINGS_KEY, POOL_BUCKET, POOL_KEY
)
from .utils import clean_col_names


@lru_cache(maxsize=None)
def load_listing_data() -> pd.DataFrame:
    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    obj = client.get_object(Bucket=LISTINGS_BUCKET, Key=LISTINGS_KEY)
    text = obj['Body'].read().decode('utf-8')
    df = pd.read_csv(io.StringIO(text), low_memory=False)

    # Numeric cleanup
    df['LATITUDE'] = pd.to_numeric(df.get('LATITUDE'), errors='coerce')
    df['LONGITUDE'] = pd.to_numeric(df.get('LONGITUDE'), errors='coerce')
    df = df.dropna(subset=['LATITUDE', 'LONGITUDE'])

    # Neighborhood extraction
    if 'NEIGHBORHOOD' not in df.columns:
        df['NEIGHBORHOOD'] = (
            df.get('ADDRESS', 'Unknown')
              .str.split(',', n=1)
              .str[0]
        )
    return df


@lru_cache
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


@lru_cache
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


@lru_cache(maxsize=None)
def load_bsky_jsons(bucket: str, prefix: str) -> list[dict]:
    client = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        region_name=AWS_REGION,
    )
    objs = client.list_objects_v2(Bucket=bucket, Prefix=prefix).get("Contents", [])
    data = []
    for obj in objs:
        if obj["Key"].endswith(".json"):
            raw = client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read()
            data.append(json.loads(raw))
    return data


@lru_cache(maxsize=None)
def process_bsky_posts(bucket: str, prefix: str, keywords: list[str]) -> pd.DataFrame:
    raws = load_bsky_jsons(bucket, prefix)
    rows = []
    for post in raws:
        text = post.get("text", "")
        rows.append({
            "account": post.get("author", {}).get("handle"),
            "descripcion": text,
            "likes":    post.get("metrics", {}).get("likes", 0),
            "reposts":  post.get("metrics", {}).get("reposts", 0),
            "replies":  post.get("metrics", {}).get("replies", 0),
            "menciona_inmuebles": (
                isinstance(text, str)
                and any(k in text.lower() for k in keywords)
            )
        })
    df = pd.DataFrame(rows)
    return df
