import json
import pandas as pd
import boto3
import io
import time
import datetime  # Explicitly import for clarity, though pandas also provides Timestamp
from kafka import KafkaConsumer
from botocore.exceptions import ClientError  # Import for S3 error handling

from src.config import (
    AWS_BUCKET_NAME,
    IDEALISTA_S3_FOLDER,  # This will now be the flat S3 folder
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_IDEALISTA,
    IDEALISTA_CONSUMER_BATCH_SIZE,
    IDEALISTA_CONSUMER_POLL_TIMEOUT,
    # AWS_REGION # Good to have for boto3 client, add if not present
)
from src.idealista.utils import is_kafka_running  # Reuse Kafka check


def create_kafka_consumer():
    """Creates and returns a Kafka consumer for the Idealista topic."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_IDEALISTA,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",
            enable_auto_commit=True,
            group_id="idealista-s3-uploader-group",
            consumer_timeout_ms=IDEALISTA_CONSUMER_POLL_TIMEOUT * 1000 * 2,
        )
        print(f"✅ Kafka consumer connected to topic '{KAFKA_TOPIC_IDEALISTA}'.")
        return consumer
    except Exception as e:
        print(f"❌ Failed to create Kafka consumer: {e}")
        return None


# Modified function signature and logic
def upload_batch_to_s3(df_batch, s3_target_folder, filename_date_str):
    """
    Uploads a DataFrame batch to S3.
    Files are stored in a single s3_target_folder.
    Filename includes the date as YYYYMMDD (e.g., listings_daily_20250523.csv).
    Appends to an existing file for the same date if it exists, or creates a new one.
    """
    if df_batch.empty:
        print(" R️ Batch is empty, skipping S3 upload.")
        return

    # Ensure AWS_REGION is available, e.g., from config or environment
    # For this example, assuming region might be configured globally for boto3
    # or you can pass it: boto3.client("s3", region_name=AWS_REGION_FROM_CONFIG)
    s3 = boto3.client("s3")

    # Create the filename with YYYYMMDD format
    # Example: listings_daily_20250523.csv (you can customize "listings_daily_")
    filename = f"listing_daily/listings_daily_{filename_date_str}.csv"

    # S3 key will be directly in the s3_target_folder
    s3_target_folder_cleaned = s3_target_folder.strip(
        "/"
    )  # Ensure no leading/trailing slashes
    key = f"{s3_target_folder_cleaned}/{filename}"

    existing_df = pd.DataFrame()
    try:
        response = s3.get_object(Bucket=AWS_BUCKET_NAME, Key=key)
        existing_csv_data = response["Body"].read().decode("utf-8")
        if existing_csv_data:  # Check if file has content
            existing_df = pd.read_csv(io.StringIO(existing_csv_data))
            print(
                f"🔄 Found existing data in s3://{AWS_BUCKET_NAME}/{key} ({len(existing_df)} rows). Appending new data."
            )
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            print(
                f"✨ No existing file found at s3://{AWS_BUCKET_NAME}/{key}. Creating new file."
            )
        else:
            print(
                f"❌ Error checking/downloading existing file {key}: {e}. Proceeding with new batch only."
            )
    except (
        pd.errors.EmptyDataError
    ):  # If file exists but is empty and unparsable by read_csv
        print(
            f"ℹ️ Existing file at s3://{AWS_BUCKET_NAME}/{key} is empty. Creating new file content."
        )
    except Exception as e:
        print(
            f"❌ Error reading existing CSV from S3 ({key}): {e}. Proceeding with new batch only."
        )

    combined_df = pd.concat([existing_df, df_batch], ignore_index=True)
    combined_df.drop_duplicates(inplace=True)  # Optional: deduplicate before saving

    csv_buffer = io.StringIO()
    combined_df.to_csv(
        csv_buffer, index=False
    )  # Always write headers for the complete daily file

    try:
        s3.put_object(Bucket=AWS_BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())
        print(
            f"✅ Uploaded combined data ({len(combined_df)} total rows) to S3 → s3://{AWS_BUCKET_NAME}/{key}"
        )
    except Exception as e:
        print(f"❌ Error uploading combined data to S3 ({key}): {e}")


def consume_and_upload_idealista():
    """Consumes messages from Kafka, batches them, and uploads to a daily S3 file."""
    print("\n--- Starting Idealista Consumer Process ---")

    consumer = create_kafka_consumer()
    if not consumer:
        print("❌ Exiting Idealista consumer due to connection failure.")
        return

    buffer = []
    batch_counter = 0  # For logging within this run
    last_message_time = time.time()

    # Determine the date for the filename ONCE when the consumer process starts.
    # All data processed in this run will go to this day's file.
    # If the consumer runs across midnight, it will continue using this start date.
    # If you need it to switch files precisely at midnight, this logic would need to be inside the loop.
    run_start_timestamp = pd.Timestamp.now()
    filename_date_str_yyyymmdd = run_start_timestamp.strftime("%Y%m%d")

    print(
        f"ℹ️ This consumer run will process data for files dated: {filename_date_str_yyyymmdd}"
    )
    print(
        f"ℹ️ Target S3 Folder for daily files: s3://{AWS_BUCKET_NAME}/{IDEALISTA_S3_FOLDER}/"
    )

    try:
        while True:
            records = consumer.poll(timeout_ms=IDEALISTA_CONSUMER_POLL_TIMEOUT * 1000)

            if not records:
                # print( # This can be very verbose if no messages
                #     f"⏳ No messages received in the last {IDEALISTA_CONSUMER_POLL_TIMEOUT}s..."
                # )
                if buffer and (
                    time.time() - last_message_time
                    > IDEALISTA_CONSUMER_POLL_TIMEOUT * 1.5
                ):
                    print(
                        f"⏰ Timeout ({IDEALISTA_CONSUMER_POLL_TIMEOUT * 1.5}s) reached with data in buffer. Processing batch {batch_counter}."
                    )
                    df = pd.DataFrame(buffer)
                    upload_batch_to_s3(
                        df,
                        IDEALISTA_S3_FOLDER,  # Pass the flat target folder
                        filename_date_str_yyyymmdd,  # Pass the YYYYMMDD date string
                    )
                    buffer.clear()
                    batch_counter += 1
                    last_message_time = time.time()
                continue

            # This print can also be verbose, consider logging levels or sampling
            # print(
            #     f"📨 Received {sum(len(msgs) for msgs in records.values())} messages."
            # )
            for tp, messages in records.items():
                for message in messages:
                    buffer.append(message.value)
                    last_message_time = time.time()

                if len(buffer) >= IDEALISTA_CONSUMER_BATCH_SIZE:
                    print(
                        f"📦 Buffer full ({len(buffer)} messages). Processing batch {batch_counter}."
                    )
                    df = pd.DataFrame(buffer)
                    upload_batch_to_s3(
                        df,
                        IDEALISTA_S3_FOLDER,  # Pass the flat target folder
                        filename_date_str_yyyymmdd,  # Pass the YYYYMMDD date string
                    )
                    buffer.clear()
                    batch_counter += 1
                    last_message_time = time.time()

    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt received. Processing final batch and exiting...")
    except Exception as e:
        print(f"❌ An error occurred during consumption: {e}")
    finally:
        if buffer:
            print(
                f"🛑 Consumer loop ended/interrupted. Processing final batch {batch_counter} ({len(buffer)} messages)."
            )
            df = pd.DataFrame(buffer)
            upload_batch_to_s3(
                df,
                IDEALISTA_S3_FOLDER,  # Pass the flat target folder
                filename_date_str_yyyymmdd,  # Pass the YYYYMMDD date string
            )
            buffer.clear()

        if consumer:
            consumer.close()
            print("🔒 Kafka consumer closed.")
        print("--- Idealista Consumer Process Finished ---")


if __name__ == "__main__":
    if is_kafka_running(KAFKA_BOOTSTRAP_SERVERS):
        consume_and_upload_idealista()
    else:
        print("❌ Kafka is not available. Idealista consumer exiting.")
