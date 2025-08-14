from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

# Define network name and app image name consistent with other DAG
DOCKER_NETWORK_NAME = "data_ingestion_project_data_ingestion_net"
APP_IMAGE_NAME = "localuser/data_ingestion_app:latest"  # Use your actual prefixed name

# Fetch necessary AWS environment variables from the Airflow worker's environment
AWS_BUCKET_NAME_VAR = os.getenv(
    "AWS_BUCKET_NAME"
)  # Potentially needed by utils.initialize_s3 if not hardcoded
AWS_REGION_VAR = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID_VAR = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY_VAR = os.getenv("AWS_SECRET_ACCESS_KEY")

# Check if essential variables are loaded (optional but good for debugging)
if not all(
    [
        AWS_BUCKET_NAME_VAR,
        AWS_REGION_VAR,
        AWS_ACCESS_KEY_ID_VAR,
        AWS_SECRET_ACCESS_KEY_VAR,
    ]
):
    print(
        "Warning: One or more AWS environment variables not found in Airflow worker environment!"
    )
    # Consider raising an error if these are critical
print(f"!!! DEBUG AWS_BUCKET_NAME_VAR: {AWS_BUCKET_NAME_VAR} !!!")
print(f"!!! DEBUG AWS_REGION_VAR: {AWS_REGION_VAR} !!!")
print(f"!!! DEBUG AWS_ACCESS_KEY_ID_VAR: {AWS_ACCESS_KEY_ID_VAR} !!!")


with DAG(
    dag_id="ajuntament_bcn",
    # schedule="0 2 * * 0",  # Run every Sunday at 02:00 UTC
    schedule="* * * * *",  # Every minute
    # Alternatively: schedule="@weekly", which often runs Sunday midnight
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),  # Adjust start date
    catchup=False,
    tags=["data-ingestion", "download", "ajuntament", "docker"],
    doc_md="""
    ### Ajuntament de Barcelona Open Data Downloader DAG

    Downloads specified datasets weekly and uploads them to S3.
    """,
) as dag:
    start = EmptyOperator(task_id="start_download")

    # run_ajuntament_downloader = DockerOperator(
    #     task_id="run_ajuntament_downloader",
    #     image=APP_IMAGE_NAME,
    #     command="python src/ajuntament_bcn/ajuntament_bcn.py",  # Command to run the new script
    #     network_mode=DOCKER_NETWORK_NAME,  # Attach to the network if needed (e.g., for internal services, though not strictly needed for S3/API)
    #     auto_remove=True,
    #     docker_url="unix://var/run/docker.sock",
    #     working_dir="/app",
    #     # Pass necessary environment variables to the container
    #     environment={
    #         "PYTHONPATH": "/app",
    #         "AWS_BUCKET_NAME": AWS_BUCKET_NAME_VAR or "",  # Needed by utils/config
    #         "AWS_REGION": AWS_REGION_VAR or "",
    #         "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID_VAR or "",
    #         "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY_VAR or "",
    #         # Add any other env vars the script or utils might need from .env
    #     },
    # )

    run_ajuntament_downloader = DockerOperator(
        task_id="run_ajuntament_downloader",
        image=APP_IMAGE_NAME,
        # --- TEMPORARILY CHANGE THIS ---
        # command='''/bin/sh -c "env | sort"''',
        command='''/bin/sh -c "python src/api/ajuntament_bcn.py"''',
        # -------------------------------
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        # Keep the environment dictionary exactly as it was, passing all fetched VARs
        environment={
            "PYTHONPATH": "/app",
            "AWS_BUCKET_NAME": AWS_BUCKET_NAME_VAR or "",
            "AWS_REGION": AWS_REGION_VAR or "",
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID_VAR or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY_VAR or "",
            # Add ALL other needed variables here if applicable
        },
    )

    end = EmptyOperator(task_id="end_download")

    # Define task dependencies

    start >> run_ajuntament_downloader >> end
