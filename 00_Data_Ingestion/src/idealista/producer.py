import pandas as pd
import json
from kafka import KafkaProducer
import time

# Import config and utilities specific to idealista
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_IDEALISTA,
    IDEALISTA_DAILY_BATCH_SIZE,
    IDEALISTA_LOCAL_REMAINING_PATH,
)
from src.idealista.utils import (
    download_remaining_from_s3,
    update_remaining_to_s3,
    is_kafka_running,
)


def create_kafka_producer():
    """Creates and returns a Kafka producer instance."""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),  # Handle list
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            retries=5,  # Add retries for robustness
            linger_ms=10,  # Batch messages slightly
        )
        print("✅ Kafka producer created successfully.")
        return producer
    except Exception as e:
        print(f"❌ Failed to create Kafka producer: {e}")
        return None


def send_listings_to_kafka(producer, df_batch):
    """Sends rows from the DataFrame batch to the Idealista Kafka topic."""
    if not producer:
        print("❌ Cannot send listings, Kafka producer is not available.")
        return False

    print(
        f"📤 Sending {len(df_batch)} listings to Kafka topic: {KAFKA_TOPIC_IDEALISTA}"
    )
    for _, row in df_batch.iterrows():
        listing = row.to_dict()
        listing_id = listing.get("id", "unknown_id")  # Use a default if 'id' is missing
        try:
            # Send message - future object is returned
            future = producer.send(KAFKA_TOPIC_IDEALISTA, value=listing)
            # Optional: Add callbacks for success/failure per message
            # future.add_callback(on_send_success).add_errback(on_send_error)
            print(f"  -> Sent listing ID: {listing_id}")
        except Exception as e:
            print(f"❌ Error sending listing ID {listing_id} to Kafka: {e}")
            # Decide if you want to stop or continue on error
            # return False # Example: stop on first error

    # Ensure all messages are sent before returning
    producer.flush()
    print("✅ Batch sent to Kafka and flushed.")
    return True


def process_daily_idealista_upload():
    """
    Main logic for the Idealista producer:
    1. Downloads the latest remaining listings.
    2. Checks if there's anything to process.
    3. Takes a batch.
    4. Sends the batch to Kafka.
    5. Updates the remaining listings file (local and S3).
    """
    print("\n--- Starting Daily Idealista Upload Process ---")

    # 1. Get the latest state
    df_remaining = download_remaining_from_s3()

    if df_remaining is None or df_remaining.empty:
        print("🎉 No remaining Idealista listings to process or error downloading.")
        return  # Exit if nothing to do or error occurred

    print(f"📰 Found {len(df_remaining)} listings remaining.")

    # 2. Take the daily batch
    batch_size = min(
        IDEALISTA_DAILY_BATCH_SIZE, len(df_remaining)
    )  # Don't take more than available
    if batch_size == 0:
        print("🎉 No remaining Idealista listings to process.")
        return
    df_daily_batch = df_remaining.iloc[:batch_size]
    print(f"⚙️ Processing batch of {len(df_daily_batch)} listings.")

    # 3. Create producer and send to Kafka
    producer = create_kafka_producer()
    if producer:
        if send_listings_to_kafka(producer, df_daily_batch):
            # 4. Update remainder only if Kafka send was successful
            df_remaining_updated = df_remaining.iloc[batch_size:]
            update_remaining_to_s3(df_remaining_updated)
        else:
            print("❌ Skipping update of remaining listings due to Kafka send failure.")
        # Close the producer
        producer.close()
        print("🛑 Kafka producer closed.")
    else:
        print("❌ Skipping upload and update due to Kafka producer creation failure.")

    print("--- Finished Daily Idealista Upload Process ---")


if __name__ == "__main__":
    # Wait for Kafka to be ready before starting the main process
    if is_kafka_running(KAFKA_BOOTSTRAP_SERVERS):
        # Add a small delay maybe, to allow consumer to start? Optional.
        # time.sleep(5)
        process_daily_idealista_upload()
    else:
        print("❌ Kafka is not available. Idealista producer exiting.")
        # Consider adding retry logic here or rely on Docker's restart policy
