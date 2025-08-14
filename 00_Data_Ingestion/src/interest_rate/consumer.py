import json
import time
from kafka import KafkaConsumer

from src.config import (
    KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_TOPIC_INTEREST_RATE,
)

# Reuse Kafka check
from src.idealista.utils import is_kafka_running


def create_kafka_consumer():
    """Create and return a Kafka consumer for interest rates with retry logic."""
    for attempt in range(1, 11):  # Retry 10 times
        try:
            consumer = KafkaConsumer(
                KAFKA_TOPIC_INTEREST_RATE,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                auto_offset_reset="earliest",
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                group_id="interest-rate-printer-group",  # Unique group ID
                consumer_timeout_ms=20000,  # Stop if idle for 20 seconds
            )
            print(f"✅ Kafka consumer for interest rates created (attempt {attempt}).")
            return consumer
        except Exception as e:
            print(f"⏳ Failed to connect Kafka consumer (attempt {attempt}/10): {e}")
            if attempt < 10:
                time.sleep(5)

    print("❌ Failed to create Kafka consumer after multiple attempts.")
    raise Exception("Failed to connect to Kafka after multiple attempts")


def consume_and_print_interest_rates():
    """Consumes and prints interest rate messages from Kafka."""
    print("\n--- Starting Interest Rate Consumer Process ---")

    consumer = create_kafka_consumer()
    if not consumer:
        print("❌ Exiting interest rate consumer due to connection failure.")
        return

    print(f"👂 Listening for messages on Kafka topic: {KAFKA_TOPIC_INTEREST_RATE}")

    try:
        for message in consumer:
            data = message.value
            timestamp = data.get("timestamp", "N/A")
            rates = data.get("rates", {})

            print(f"\nReceived @ {message.timestamp} | Timestamp in data: {timestamp}")
            print("  Interest Rates:")
            for rate_type, value in rates.items():
                print(f"    {rate_type}: {value}%")

    except KeyboardInterrupt:
        print("\n🛑 KeyboardInterrupt received. Shutting down interest rate consumer.")
    except Exception as e:
        print(f"❌ An error occurred during consumption: {e}")
    finally:
        if consumer:
            consumer.close()
        print("--- Interest Rate Consumer Process Finished ---")


if __name__ == "__main__":
    if is_kafka_running(KAFKA_BOOTSTRAP_SERVERS):
        consume_and_print_interest_rates()
    else:
        print("❌ Kafka is not available. Interest rate consumer exiting.")
