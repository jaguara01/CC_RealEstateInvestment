# modules/config.py

import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# AWS Credentials and Region
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION            = os.getenv("AWS_REGION", "eu-west-1")

# S3 Buckets and Keys
# Listings enriched dataset
LISTINGS_BUCKET = "01-structured"
LISTINGS_KEY    = "property-listings/idealista/Barcelona_Sale_enriched.csv"

# Remaining listings pool (for the investment analyzer)
POOL_BUCKET = "01-structured"
POOL_KEY    = "property-listings/idealista/remaining_listings/remaining.csv"

# Model storage
MODEL_BUCKET = "07-model"
MODEL_KEY    = "property_price_model_rf_log_target_simple.joblib"

# Feature list for model
FEATURES_KEY = "model_features_rf_log_target_simple.json"

# Sentiment data
SENT_BUCKET   = os.getenv("S3_BUCKET", "stream-worker-files")
SENT_PREFIX   = "posts_bluesky/"
SENT_KEYWORDS = [
    "venta", "apartamento", "casa", "pisos", "inmueble",
    "inmobiliaria", "barcelona", "bcn", "compra", "renta"
]

# Aliases for the Financial Analyzer page
S3_MODEL_BUCKET_NAME    = MODEL_BUCKET
S3_DATA_BUCKET_NAME     = POOL_BUCKET
MODEL_S3_KEY            = MODEL_KEY
FEATURES_S3_KEY         = FEATURES_KEY
PROPERTY_DATA_S3_KEY    = POOL_KEY

# Validate critical config
required_vars = [AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY]
if not all(required_vars):
    missing = [
        name
        for name, val in zip(
            ["AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"],
            required_vars
        )
        if not val
    ]
    raise EnvironmentError(
        f"Missing required environment variables: {', '.join(missing)}"
    )
