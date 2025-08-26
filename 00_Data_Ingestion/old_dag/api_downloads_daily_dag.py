# dags/api_downloads_daily_dag.py

from __future__ import annotations

import pendulum
import os

from airflow.models.dag import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.operators.empty import EmptyOperator

# --- Use the same constants as your other DAGs ---
DOCKER_NETWORK_NAME = "data_ingestion_project_data_ingestion_net"
APP_IMAGE_NAME = "localuser/data_ingestion_app:latest"  # Use your actual prefixed name
# ------------------------------------------------

# --- Fetch ALL environment variables potentially needed by ANY script in src/api ---
print(
    "--- Reading Env Vars in api_downloads_daily_dag ---"
)  # For scheduler log debugging
AWS_BUCKET_NAME_VAR = os.getenv("AWS_BUCKET_NAME")
AWS_REGION_VAR = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID_VAR = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY_VAR = os.getenv("AWS_SECRET_ACCESS_KEY")
# TODO: Add os.getenv() calls for ALL API keys defined in your .env for these scripts
GOOGLE_PLACES_API_KEY_VAR = os.getenv("GOOGLE_PLACES_API_KEY")
NEWSAPI_API_KEY_VAR = os.getenv("NEWSAPI_API_KEY")
BLUESKY_USERNAME_VAR = os.getenv("BLUESKY_USERNAME")
BLUESKY_PASSWORD_VAR = os.getenv("BLUESKY_PASSWORD")
# Add others... e.g., ECB_API_KEY_VAR = os.getenv('ECB_API_KEY')

# Print fetched values for debugging in scheduler logs
print(f"AWS_BUCKET_NAME: {AWS_BUCKET_NAME_VAR}")
print(f"AWS_REGION: {AWS_REGION_VAR}")
print(f"AWS_ACCESS_KEY_ID: {AWS_ACCESS_KEY_ID_VAR}")
print(f"AWS_SECRET_ACCESS_KEY is set: {bool(AWS_SECRET_ACCESS_KEY_VAR)}")
print(f"GOOGLE_PLACES_API_KEY is set: {bool(GOOGLE_PLACES_API_KEY_VAR)}")
print(f"NEWSAPI_API_KEY is set: {bool(NEWSAPI_API_KEY_VAR)}")
print(f"BLUESKY_USERNAME: {BLUESKY_USERNAME_VAR}")
print(f"BLUESKY_PASSWORD is set: {bool(BLUESKY_PASSWORD_VAR)}")
print("---------------------------------------------")
# -----------------------------------------------------------------------------------

# --- Environment dictionary to pass to ALL tasks in this DAG ---
# --- Ensure ALL necessary variables are included here! ---
TASK_ENVIRONMENT = {
    "PYTHONPATH": "/app",
    "AWS_BUCKET_NAME": AWS_BUCKET_NAME_VAR or "",
    "AWS_REGION": AWS_REGION_VAR or "",
    "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID_VAR or "",
    "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY_VAR or "",
    # TODO: Add ALL API Keys needed by the scripts below
    "GOOGLE_PLACES_API_KEY": GOOGLE_PLACES_API_KEY_VAR or "",
    "NEWSAPI_API_KEY": NEWSAPI_API_KEY_VAR or "",
    "BLUESKY_USERNAME": BLUESKY_USERNAME_VAR or "",
    "BLUESKY_PASSWORD": BLUESKY_PASSWORD_VAR or "",
    # Add others... 'ECB_API_KEY': ECB_API_KEY_VAR or '',
}
# ----------------------------------------------------------


with DAG(
    dag_id="api_downloads_daily",  # Unique ID for this DAG
    schedule="@daily",  # Run once daily at midnight UTC
    # Or use cron: '0 4 * * *' # Run every day at 04:00 UTC
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),  # Adjust start date
    catchup=False,
    tags=["data-ingestion", "download", "api", "docker", "daily"],
    doc_md="""### Daily API Data Download DAG""",
) as dag:

    start = EmptyOperator(task_id="start_daily_downloads")

    # --- Define ONE DockerOperator task for EACH script ---

    # run_ajuntament_bcn = DockerOperator(
    #     task_id="run_ajuntament_bcn",
    #     image=APP_IMAGE_NAME,
    #     command='''/bin/sh -c "python src/api/ajuntament_bcn.py"''',  # Updated path
    #     network_mode=DOCKER_NETWORK_NAME,
    #     auto_remove=True,
    #     docker_url="unix://var/run/docker.sock",
    #     working_dir="/app",
    #     environment=TASK_ENVIRONMENT,  # Pass the common environment dict
    # )

    # run_ecb = DockerOperator(
    #     task_id="run_ecb",
    #     image=APP_IMAGE_NAME,
    #     command='''/bin/sh -c "python src/api/ecb.py"''',  # Check filename
    #     network_mode=DOCKER_NETWORK_NAME,
    #     auto_remove=True,
    #     docker_url="unix://var/run/docker.sock",
    #     working_dir="/app",
    #     environment=TASK_ENVIRONMENT,
    # )

    run_european_central_bank = DockerOperator(
        task_id="run_european_central_bank",  # Example - adjust task_id
        image=APP_IMAGE_NAME,
        command='''/bin/sh -c "python src/api/European_Central_Bank.py"''',  # Check filename
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        environment=TASK_ENVIRONMENT,
    )

    run_google_places = DockerOperator(
        task_id="run_google_places",  # Example - adjust task_id
        image=APP_IMAGE_NAME,
        command='''/bin/sh -c "python src/api/google_places.py"''',  # Check filename
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        environment=TASK_ENVIRONMENT,
    )

    run_ine = DockerOperator(
        task_id="run_ine",  # Example - adjust task_id
        image=APP_IMAGE_NAME,
        command='''/bin/sh -c "python src/api/INE.py"''',  # Check filename
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        environment=TASK_ENVIRONMENT,
    )

    run_newsapi = DockerOperator(
        task_id="run_newsapi",  # Example - adjust task_id
        image=APP_IMAGE_NAME,
        command='''/bin/sh -c "python src/api/Newsapi.py"''',  # Check filename
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        environment=TASK_ENVIRONMENT,
    )

    run_overpass_osm = DockerOperator(
        task_id="run_overpass_osm",  # Example - adjust task_id
        image=APP_IMAGE_NAME,
        command='''/bin/sh -c "python src/api/Overpass_OpenStreetMap.py"''',  # Check filename
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        environment=TASK_ENVIRONMENT,
    )

    run_bluesky = DockerOperator(  # Added Bluesky example
        task_id="run_bluesky",
        image=APP_IMAGE_NAME,
        command='''/bin/sh -c "python src/api/Bluesky.py"''',  # Check filename
        network_mode=DOCKER_NETWORK_NAME,
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",
        environment=TASK_ENVIRONMENT,
    )

    # TODO: Add a DockerOperator task for any other relevant scripts in src/api
    # Make sure the `command` path and `task_id` are correct for each.

    end = EmptyOperator(task_id="end_daily_downloads")

    # Define task dependencies - run all API downloads in parallel
    (
        start
        >> [
            # run_ajuntament_bcn,
            # run_ecb,
            run_european_central_bank,
            run_google_places,
            run_ine,
            run_newsapi,
            run_overpass_osm,
            run_bluesky,
            # Add other task variables here
        ]
        >> end
    )
