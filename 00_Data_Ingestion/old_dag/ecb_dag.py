# dags/ecb_downloader_dag.py

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

# --- Fetch ALL environment variables ECB.py might need ---
# Example: Assume it also needs S3 access via utils.py
AWS_BUCKET_NAME_VAR = os.getenv("AWS_BUCKET_NAME")
AWS_REGION_VAR = os.getenv("AWS_REGION")
AWS_ACCESS_KEY_ID_VAR = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY_VAR = os.getenv("AWS_SECRET_ACCESS_KEY")
# Add any other specific variables ECB.py or its imports might use
# ECB_API_KEY_VAR = os.getenv('ECB_API_KEY') # Fictional example
# -----------------------------------------------------

with DAG(
    dag_id="ecb_data_downloader",  # Unique ID for this DAG
    schedule="0 3 * * *",  # Run every day at 03:00 UTC (adjust as needed)
    # Alternatively: schedule="@daily", which runs at midnight UTC
    start_date=pendulum.datetime(2023, 10, 1, tz="UTC"),  # Adjust start date
    catchup=False,
    tags=["data-ingestion", "download", "ecb", "docker"],
    doc_md="""### ECB Data Downloader DAG""",
) as dag:
    start = EmptyOperator(task_id="start_ecb_download")

    run_ecb_downloader = DockerOperator(
        task_id="run_ecb_downloader",
        image=APP_IMAGE_NAME,
        # --- Use the correct path to your new script ---
        command='''/bin/sh -c "python src/ajuntament_bcn/ecb.py"''',
        # -----------------------------------------------
        network_mode=DOCKER_NETWORK_NAME,  # Include if it needs to talk to Kafka/other services
        # May not be needed if only accessing external APIs/S3
        auto_remove=True,
        docker_url="unix://var/run/docker.sock",
        working_dir="/app",  # Important for imports like 'from src.utils ...'
        # --- Pass ALL required environment variables ---
        environment={
            "PYTHONPATH": "/app",
            "AWS_BUCKET_NAME": AWS_BUCKET_NAME_VAR or "",
            "AWS_REGION": AWS_REGION_VAR or "",
            "AWS_ACCESS_KEY_ID": AWS_ACCESS_KEY_ID_VAR or "",
            "AWS_SECRET_ACCESS_KEY": AWS_SECRET_ACCESS_KEY_VAR or "",
            # Add other specific vars needed by ECB.py
            # 'ECB_API_KEY': ECB_API_KEY_VAR or '', # Fictional example
        },
        # --------------------------------------------
    )

    end = EmptyOperator(task_id="end_ecb_download")

    # Define task dependencies
    start >> run_ecb_downloader >> end
