import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables from .env file, searching upwards from this file
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

# Kafka Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC_IDEALISTA = os.getenv("KAFKA_TOPIC_IDEALISTA", "idealista_listings")
KAFKA_TOPIC_INTEREST_RATE = os.getenv("KAFKA_TOPIC_INTEREST_RATE", "interest_rates")

# AWS Configuration
AWS_REGION = os.getenv("AWS_REGION", "eu-north-1")
AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")  # Required
AWS_ACCESS_KEY_ID = os.getenv(
    "AWS_ACCESS_KEY_ID"
)  # Optional if using IAM roles/profiles
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")  # Optional

# Idealista Configuration
IDEALISTA_S3_FOLDER = os.getenv("IDEALISTA_S3_FOLDER", "property-listings/idealista")
IDEALISTA_REMAINING_FILE_KEY = os.getenv(
    "IDEALISTA_REMAINING_FILE_KEY", "remaining_listings/remaining.csv"
)
IDEALISTA_DAILY_BATCH_SIZE = int(os.getenv("IDEALISTA_DAILY_BATCH_SIZE", 5))
IDEALISTA_CONSUMER_BATCH_SIZE = int(os.getenv("IDEALISTA_CONSUMER_BATCH_SIZE", 5))
IDEALISTA_CONSUMER_POLL_TIMEOUT = int(os.getenv("IDEALISTA_CONSUMER_POLL_TIMEOUT", 10))
# Define path relative to project root (assuming src is one level down)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
IDEALISTA_LOCAL_REMAINING_PATH = PROJECT_ROOT / "data" / "remaining.csv"

# Interest Rate Configuration
INTEREST_RATE_S3_FOLDER = os.getenv(
    "INTEREST_RATE_S3_FOLDER", "financial-data/interest_rate"
)
INTEREST_RATE_BATCH_SIZE = int(os.getenv("INTEREST_RATE_BATCH_SIZE", 20))
INTEREST_RATE_SIMULATION_INTERVAL_MIN = float(
    os.getenv("INTEREST_RATE_SIMULATION_INTERVAL_MIN", 0.5)
)
INTEREST_RATE_SIMULATION_INTERVAL_MAX = float(
    os.getenv("INTEREST_RATE_SIMULATION_INTERVAL_MAX", 2.0)
)

# --- Input Validation (Optional but recommended) ---
if not AWS_BUCKET_NAME:
    raise ValueError("Error: AWS_BUCKET_NAME environment variable is not set.")

# Ensure the directory for the local remaining file exists
IDEALISTA_LOCAL_REMAINING_PATH.parent.mkdir(parents=True, exist_ok=True)

print("--- Configuration Loaded ---")
print(f"Kafka Servers: {KAFKA_BOOTSTRAP_SERVERS}")
print(f"AWS Bucket: {AWS_BUCKET_NAME}")
print(f"Idealista Topic: {KAFKA_TOPIC_IDEALISTA}")
print(f"Interest Rate Topic: {KAFKA_TOPIC_INTEREST_RATE}")
print(f"Local Idealista Path: {IDEALISTA_LOCAL_REMAINING_PATH}")
print("--------------------------")
