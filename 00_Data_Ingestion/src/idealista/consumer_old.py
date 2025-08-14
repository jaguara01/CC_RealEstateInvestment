import json
import pandas as pd
import boto3
import io
import time
from kafka import KafkaConsumer

from src.config import (
    AWS_BUCKET_NAME,
    IDEALISTA_S3_FOLDER,
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_IDEALISTA,
    IDEALISTA_CONSUMER_BATCH_SIZE,
    IDEALISTA_CONSUMER_POLL_TIMEOUT,
)
from src.idealista.utils import is_kafka_running  # Reuse Kafka check


def create_kafka_consumer():
    """Creates and returns a Kafka consumer for the Idealista topic."""
    try:
        consumer = KafkaConsumer(
            KAFKA_TOPIC_IDEALISTA,
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="earliest",  # Start reading from the beginning if no offset found
            enable_auto_commit=True,  # Commit offsets automatically after processing
            group_id="idealista-s3-uploader-group",  # Consumer group ID
            consumer_timeout_ms=IDEALISTA_CONSUMER_POLL_TIMEOUT
            * 1000
            * 2,  # Stop if no messages for a while
            # security_protocol='SASL_SSL', # Example for secured Kafka
            # sasl_mechanism='PLAIN',
            # sasl_plain_username='user',
            # sasl_plain_password='pw',
        )
        print(f"✅ Kafka consumer connected to topic '{KAFKA_TOPIC_IDEALISTA}'.")
        return consumer
    except Exception as e:
        print(f"❌ Failed to create Kafka consumer: {e}")
        return None


def upload_batch_to_s3(df_batch, batch_id):
    """Uploads a DataFrame batch to S3 in the Idealista folder."""
    if df_batch.empty:
        print(" R️ Batch is empty, skipping S3 upload.")
        return

    s3 = boto3.client("s3")
    timestamp = pd.Timestamp.now().strftime("%Y%m%d_%H%M%S")
    # Example: idealista_data/from_kafka/2023/10/27/listings_batch_0_20231027_153000.csv
    date_prefix = pd.Timestamp.now().strftime("%Y/%m/%d")
    filename = f"from_kafka/{date_prefix}/listings_batch_{batch_id}_{timestamp}.csv"
    key = f"{IDEALISTA_S3_FOLDER}/{filename}"

    csv_buffer = io.StringIO()
    df_batch.to_csv(csv_buffer, index=False)

    try:
        s3.put_object(Bucket=AWS_BUCKET_NAME, Key=key, Body=csv_buffer.getvalue())
        print(
            f"✅ Uploaded batch {batch_id} ({len(df_batch)} rows) to S3 → s3://{AWS_BUCKET_NAME}/{key}"
        )
    except Exception as e:
        print(f"❌ Error uploading batch {batch_id} to S3 ({key}): {e}")


def consume_and_upload_idealista():
    """Consumes messages from Kafka, batches them, and uploads to S3."""
    print("\n--- Starting Idealista Consumer Process ---")

    consumer = create_kafka_consumer()
    if not consumer:
        print("❌ Exiting Idealista consumer due to connection failure.")
        return

    buffer = []
    batch_id = 0
    last_message_time = time.time()

    try:
        while True:
            # Poll for messages with a timeout
            # Adjust poll timeout based on expected message frequency
            records = consumer.poll(timeout_ms=IDEALISTA_CONSUMER_POLL_TIMEOUT * 1000)

            if not records:
                print(
                    f"⏳ No messages received in the last {IDEALISTA_CONSUMER_POLL_TIMEOUT}s..."
                )
                # If buffer has data and it's been a while, process it
                if buffer and (
                    time.time() - last_message_time
                    > IDEALISTA_CONSUMER_POLL_TIMEOUT * 1.5
                ):
                    print(
                        "⏰ Timeout reached with data in buffer. Processing remaining batch."
                    )
                    df = pd.DataFrame(buffer)
                    upload_batch_to_s3(df, batch_id)
                    buffer.clear()
                    batch_id += 1
                    last_message_time = time.time()  # Reset timer
                continue  # Go back to polling

            print(
                f"📨 Received {sum(len(msgs) for msgs in records.values())} messages."
            )
            for tp, messages in records.items():
                for message in messages:
                    buffer.append(message.value)
                    last_message_time = time.time()  # Update time on message receipt

                    # Process buffer if it reaches batch size
                    if len(buffer) >= IDEALISTA_CONSUMER_BATCH_SIZE:
                        print(
                            f"📦 Buffer full ({len(buffer)} messages). Processing batch {batch_id}."
                        )
                        df = pd.DataFrame(buffer)
                        upload_batch_to_s3(df, batch_id)
                        buffer.clear()
                        batch_id += 1
                        last_message_time = time.time()  # Reset timer after processing

    except Exception as e:
        print(f"❌ An error occurred during consumption: {e}")
    finally:
        # Process any remaining messages in the buffer before exiting
        if buffer:
            print("🛑 Consumer loop ended. Processing final batch.")
            df = pd.DataFrame(buffer)
            upload_batch_to_s3(df, batch_id)

        if consumer:
            consumer.close()
        print("--- Idealista Consumer Process Finished ---")


if __name__ == "__main__":
    # Wait for Kafka to be ready
    if is_kafka_running(KAFKA_BOOTSTRAP_SERVERS):
        consume_and_upload_idealista()
    else:
        print("❌ Kafka is not available. Idealista consumer exiting.")
