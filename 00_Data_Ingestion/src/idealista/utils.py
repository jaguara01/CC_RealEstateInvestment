import pandas as pd
import boto3
import io
import time
from kafka.admin import KafkaAdminClient
from kafka.errors import NoBrokersAvailable

from src.config import (
    AWS_BUCKET_NAME,
    IDEALISTA_S3_FOLDER,
    IDEALISTA_REMAINING_FILE_KEY,
    IDEALISTA_LOCAL_REMAINING_PATH,
)

# Initialize S3 client globally or within functions as needed
s3 = boto3.client("s3")


def get_idealista_s3_key(filename):
    """Constructs the S3 key for Idealista files."""
    return f"{IDEALISTA_S3_FOLDER}/{filename}"


def download_remaining_from_s3():
    """
    Downloads the latest remaining.csv from S3 for Idealista data
    and stores it locally. Returns the DataFrame.
    """
    s3_key = get_idealista_s3_key(IDEALISTA_REMAINING_FILE_KEY)
    local_path = IDEALISTA_LOCAL_REMAINING_PATH
    try:
        print(
            f"📥 Downloading Idealista remaining file from S3: s3://{AWS_BUCKET_NAME}/{s3_key}"
        )
        response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=s3_key)
        df = pd.read_csv(io.BytesIO(response["Body"].read()))
        df.to_csv(local_path, index=False)
        print(f"✅ Idealista local file updated: {local_path} ({len(df)} rows)")
        return df
    except s3.exceptions.NoSuchKey:
        print(
            f"❌ Idealista remaining file not found in S3: {s3_key}. Creating empty local file."
        )
        df = pd.DataFrame()  # Create empty DataFrame
        df.to_csv(local_path, index=False)  # Save empty CSV
        return df
    except Exception as e:
        print(f"❌ Error downloading {s3_key}: {e}")
        # Decide how to handle error - maybe return None or raise exception
        return pd.DataFrame()  # Return empty dataframe on error


def update_remaining_to_s3(df_remaining):
    """
    Updates remaining.csv in S3 and locally for Idealista data.
    """
    s3_key = get_idealista_s3_key(IDEALISTA_REMAINING_FILE_KEY)
    local_path = IDEALISTA_LOCAL_REMAINING_PATH

    csv_buffer = io.StringIO()
    df_remaining.to_csv(csv_buffer, index=False)

    try:
        # Upload to S3
        s3.put_object(Bucket=AWS_BUCKET_NAME, Key=s3_key, Body=csv_buffer.getvalue())
        # Save locally
        df_remaining.to_csv(local_path, index=False)
        print(
            f"🔁 Updated Idealista remaining.csv (S3 & Local) → {len(df_remaining)} rows left"
        )
    except Exception as e:
        print(f"❌ Error updating remaining.csv to S3 ({s3_key}): {e}")


def is_kafka_running(bootstrap_servers, retries=5, delay=3):
    """Checks if Kafka brokers are available."""
    servers_list = bootstrap_servers.split(",")  # Handle multiple servers if provided
    print(f"⏳ Checking Kafka connection to: {servers_list}...")
    for attempt in range(1, retries + 1):
        try:
            admin = KafkaAdminClient(bootstrap_servers=servers_list)
            topics = admin.list_topics()  # A simple operation to test connection
            print(f"✅ Kafka is running. Available topics: {topics}")
            admin.close()
            return True
        except NoBrokersAvailable:
            print(
                f"⏳ Kafka not ready (attempt {attempt}/{retries}). Brokers not available. Retrying in {delay}s..."
            )
            time.sleep(delay)
        except Exception as e:
            print(f"❌ Kafka connection error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                print(f"Retrying in {delay}s...")
                time.sleep(delay)
            else:
                admin.close()
                return False  # Failed after retries

    print(f"❌ Kafka still not available at {servers_list} after {retries} retries.")
    return False
