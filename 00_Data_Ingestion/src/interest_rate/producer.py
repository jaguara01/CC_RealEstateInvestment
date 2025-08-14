import json
import time
import random
import datetime
import numpy as np
import boto3
from kafka import KafkaProducer
from io import StringIO

# Import config and Kafka check utility
from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INTEREST_RATE,
    AWS_REGION,
    AWS_BUCKET_NAME,
    INTEREST_RATE_BATCH_SIZE,
    INTEREST_RATE_SIMULATION_INTERVAL_MIN,
    INTEREST_RATE_SIMULATION_INTERVAL_MAX,
    INTEREST_RATE_S3_FOLDER,
)

# Can reuse the Kafka check from idealista utils or create a common one
from src.idealista.utils import is_kafka_running


def create_kafka_producer():
    """Create and return a Kafka producer for interest rates."""
    # Retry connection until Kafka is ready
    for attempt in range(1, 11):  # Retry 10 times
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                linger_ms=10,
            )
            print(f"✅ Kafka producer for interest rates created (attempt {attempt}).")
            return producer
        except Exception as e:
            print(f"⏳ Failed to connect Kafka producer (attempt {attempt}/10): {e}")
            if attempt < 10:
                time.sleep(5)

    print("❌ Failed to create Kafka producer after multiple attempts.")
    raise Exception("Failed to connect to Kafka after multiple attempts")


def get_s3_client():
    """Create and return an S3 client."""
    return boto3.client("s3", region_name=AWS_REGION)


def upload_batch_to_s3(data_batch, batch_id):
    """Upload a batch of interest rate data to S3."""
    if not AWS_BUCKET_NAME:
        print("⚠️ S3_BUCKET_NAME not set, skipping S3 upload for interest rates.")
        return
    if not data_batch:
        print(" R️ Interest rate batch is empty, skipping S3 upload.")
        return

    s3_client = get_s3_client()

    # Convert data to JSON Lines format
    jsonl_data = StringIO()
    for item in data_batch:
        jsonl_data.write(json.dumps(item) + "\n")
    jsonl_data.seek(0)  # Reset buffer position to the beginning

    # Create a timestamp-based path structure
    now = datetime.datetime.now()
    date_prefix = now.strftime("%Y/%m/%d")
    timestamp_file = now.strftime("%H%M%S")

    # Create S3 key with partitioning
    s3_key = f"{INTEREST_RATE_S3_FOLDER}/{date_prefix}/rates_{timestamp_file}_batch_{batch_id}.jsonl"

    try:
        s3_client.put_object(
            Bucket=AWS_BUCKET_NAME, Key=s3_key, Body=jsonl_data.getvalue()
        )
        print(
            f"✅ Uploaded interest rate batch {batch_id} ({len(data_batch)} records) to s3://{AWS_BUCKET_NAME}/{s3_key}"
        )
    except Exception as e:
        print(
            f"❌ Error uploading interest rate batch {batch_id} to S3 ({s3_key}): {e}"
        )


def simulate_interest_rates():
    """Simulates a single interest rate data point."""
    base_rates = {
        "federal_funds_rate": 5.25,
        "prime_rate": 8.00,
        "mortgage_30y": 6.75,
        "mortgage_15y": 6.00,
        "auto_loan_60m": 7.50,
        "credit_card": 21.50,
        "personal_loan": 11.25,
        "savings_account": 1.25,
    }
    volatility = {
        "federal_funds_rate": 0.02,
        "prime_rate": 0.03,
        "mortgage_30y": 0.05,
        "mortgage_15y": 0.04,
        "auto_loan_60m": 0.06,
        "credit_card": 0.10,
        "personal_loan": 0.08,
        "savings_account": 0.01,
    }
    current_rates = base_rates.copy()
    print("📈 Simulating a single interest rate data point...")

    timestamp = datetime.datetime.now().isoformat()
    for rate_type in current_rates:
        adjustment = np.random.normal(0, volatility[rate_type])
        current_rates[rate_type] = max(
            0.1, current_rates[rate_type] + adjustment
        )  # Min rate 0.1%
        if random.random() < 0.05:  # 5% chance of bigger jump
            event_adjustment = random.choice([-0.25, -0.15, 0.15, 0.25])
            current_rates[rate_type] = max(
                0.1, current_rates[rate_type] + event_adjustment
            )

    # Ensure prime rate stays related to federal funds rate
    current_rates["prime_rate"] = max(
        0.1, current_rates["federal_funds_rate"] + random.uniform(2.75, 3.25)
    )

    data = {
        "timestamp": timestamp,
        "rates": {k: round(v, 2) for k, v in current_rates.items()},
    }
    print(f"Simulated data: {data}")
    return data  # Return the single data point


def run_interest_rate_producer():
    """Main function to run the interest rate producer for a single data point."""
    print("\n--- Starting Interest Rate Producer Process (Single Rate Mode) ---")

    producer = None  # Initialize producer to None for robust finally block
    data_batch = []
    # batch_count isn't strictly necessary if only one batch is expected,
    # but kept for compatibility with upload_batch_to_s3 signature.
    # We can use a more descriptive batch_id for the single upload.
    batch_id_for_s3 = "single_rate_data"

    try:
        producer = (
            create_kafka_producer()
        )  # This can raise an exception if Kafka is not available
        if not producer:
            # create_kafka_producer now raises an exception if it fails after retries,
            # so this 'if not producer' might not be reached if it's unhandled above.
            # However, keeping it for safety or if create_kafka_producer is changed.
            print(
                "❌ Exiting interest rate producer due to Kafka connection failure handled in run function."
            )
            return

        print(
            f"🔗 Connected to Kafka, producing interest rate to topic: {KAFKA_TOPIC_INTEREST_RATE}"
        )

        single_rate_data = simulate_interest_rates()  # Get the single data point

        if single_rate_data:
            # Send to Kafka
            try:
                producer.send(KAFKA_TOPIC_INTEREST_RATE, single_rate_data)
                print(f"  -> Sent to Kafka: {single_rate_data['timestamp']}")
            except Exception as e:
                print(f"❌ Error sending interest rate data to Kafka: {e}")
                # Decide if this error should prevent S3 upload or script continuation

            # Add to S3 batch
            data_batch.append(single_rate_data)

            # If INTEREST_RATE_BATCH_SIZE is 1, this will upload immediately.
            # Otherwise, the finally block will handle it.
            if len(data_batch) >= INTEREST_RATE_BATCH_SIZE:
                upload_batch_to_s3(data_batch, batch_id_for_s3)
                if producer:  # Check if producer exists before flushing
                    producer.flush()  # Ensure Kafka message for this batch is sent
                data_batch = []  # Reset the batch

    except KeyboardInterrupt:  # This might not be triggered by Airflow DockerOperator
        print("\n🛑 KeyboardInterrupt received. Shutting down interest rate producer.")
    except Exception as e:
        print(f"❌ An unexpected error occurred in the interest rate producer: {e}")
    finally:
        # Upload any remaining data in the batch (will be the single item if not uploaded above)
        if data_batch:  # Check if data_batch is not empty
            print("🔄 Uploading final interest rate data to S3...")
            upload_batch_to_s3(
                data_batch, f"final_{batch_id_for_s3}"
            )  # Use a distinct ID

        if producer:
            print("⏳ Flushing final Kafka messages...")
            producer.flush()  # Ensure the single Kafka message is flushed
            producer.close()
            print("🛑 Kafka producer for interest rates closed.")
    print(
        "--- Interest Rate Producer Process Finished ---"
    )  # This line will now be reached


if __name__ == "__main__":
    if is_kafka_running(KAFKA_BOOTSTRAP_SERVERS):
        run_interest_rate_producer()
    else:
        print("❌ Kafka is not available. Interest rate producer exiting.")
