# dags/producer_scheduler_dag.py

from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG

# --- Import DockerOperator ---
from airflow.providers.docker.operators.docker import DockerOperator

# -----------------------------
from airflow.operators.empty import EmptyOperator

# Note: PROJECT_DIR and COMPOSE_COMMAND are no longer needed for this approach

# Define the name of the Docker network created by docker-compose
# Check your docker-compose.yml, often it's <project_folder>_default or the custom name
# Use 'docker network ls' while compose is up to confirm, or the 'networks' section in compose
DOCKER_NETWORK_NAME = "data_ingestion_project_data_ingestion_net"  # Matches the network name in docker-compose.yml
APP_IMAGE_NAME = "localuser/data_ingestion_app:latest"  # Matches the image name set in docker-compose.yml
AWS_BUCKET_NAME_VAR = os.getenv("AWS_BUCKET_NAME")
AWS_REGION_VAR = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID_VAR = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY_VAR = os.getenv("AWS_SECRET_ACCESS_KEY")
KAFKA_BOOTSTRAP_SERVERS_VAR = os.getenv("KAFKA_BOOTSTRAP_SERVERS")

print("--- Checking Environment in DAG File ---")
# Print ALL variables you fetch with os.getenv
print(f"AWS_BUCKET_NAME: {os.getenv('AWS_BUCKET_NAME')}")
print(f"KAFKA_BOOTSTRAP_SERVERS: {os.getenv('KAFKA_BOOTSTRAP_SERVERS')}")
# ... etc ...
print("------------------------------------")


with DAG(
    dag_id="data_producer_scheduler",
    schedule="*/1 * * * *",  # Every minute
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),
    catchup=False,
    tags=["data-ingestion", "producer", "docker"],
    doc_md="""
    ### Data Producer Scheduler DAG

    Schedules the execution of Idealista and Interest Rate data producers
    using the DockerOperator.
    """,
) as dag:
    start = EmptyOperator(task_id="start")

    # --- Idealista Producer Task using DockerOperator ---
    run_idealista_producer = DockerOperator(
        task_id="run_idealista_producer",
        image=APP_IMAGE_NAME,
        # Command to run inside the new container
        # command='''/bin/sh -c "env | sort"''',
        # command="python src/idealista/producer.py",
        command='''/bin/sh -c "python src/idealista/producer.py"''',
        network_mode=DOCKER_NETWORK_NAME,  # Attach to the same network as Kafka/Postgres
        auto_remove=True,  # Equivalent of --rm
        docker_url="unix://var/run/docker.sock",  # Explicitly point to the mounted socket
        working_dir="/app",
        environment={
            "PYTHONPATH": "/app",
            "AWS_BUCKET_NAME": AWS_BUCKET_NAME_VAR or "",  # Pass fetched value
            "AWS_REGION": AWS_REGION_VAR or "",
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID_VAR or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY_VAR or "",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS_VAR or "kafka:29092",
        },  # Add /app to Python's module search path
        # Ensures operator uses the Docker daemon on the host
    )

    # --- Interest Rate Producer Task using DockerOperator ---
    run_interest_rate_producer = DockerOperator(
        task_id="run_interest_rate_producer",
        image=APP_IMAGE_NAME,
        command="python src/interest_rate/producer.py",
        # command='''/bin/sh -c "python src/interest_rate/producer.py"''',
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        environment={
            "PYTHONPATH": "/app",
            "AWS_BUCKET_NAME": AWS_BUCKET_NAME_VAR or "",  # Pass fetched value
            "AWS_REGION": AWS_REGION_VAR or "",
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID_VAR or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY_VAR or "",
            "KAFKA_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS_VAR or "kafka:29092",
        },  # Add /app to Python's module search path
        # environment={} # Add specific env vars if needed
    )

    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> [run_idealista_producer, run_interest_rate_producer] >> end
