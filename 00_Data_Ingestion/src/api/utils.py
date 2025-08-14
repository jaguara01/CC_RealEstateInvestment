import os
from dotenv import load_dotenv

import boto3
import os
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Import necessary configuration from the central config file
# This assumes config.py reads these from the environment variables
# which are set via .env and passed by DockerOperator/DockerCompose
try:
    from src.config import AWS_REGION, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
except ImportError:
    # Provide fallbacks or raise an error if config isn't found during basic use
    print(
        "Warning: Could not import AWS config from src.config. Using environment directly or defaults."
    )
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")  # Default region
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

# --- S3 Functions ---


def initialize_s3():
    """
    Initializes and returns a boto3 S3 client using credentials
    expected to be available as environment variables (via config).
    """
    try:
        access_key = AWS_ACCESS_KEY_ID
        secret_key = AWS_SECRET_ACCESS_KEY
        region = AWS_REGION

        if not access_key or not secret_key:
            print(
                "❌ Error: AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY not found in environment/config."
            )
            return None

        if not region:
            print(
                "⚠️ Warning: AWS_REGION not found in environment/config. Using default 'us-east-1'."
            )
            region = "us-east-1"

        session = boto3.Session(
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )
        s3_client = session.client("s3")
        print("✅ S3 client initialized successfully.")
        return s3_client
    # Catching potential issues during session/client creation
    except (NoCredentialsError, PartialCredentialsError):
        print("❌ Boto3 could not find complete AWS credentials.")
        return None
    except ClientError as e:
        print(f"❌ AWS ClientError during S3 client initialization: {e}")
        return None
    except Exception as e:
        print(f"❌ Failed to initialize S3 client: {e}")
        return None


def upload_to_s3(s3_client, local_filepath, s3_key, bucket_name):
    """
    Uploads a local file to a specific S3 key and bucket, with error handling.
    Removes the local file on successful upload.
    """
    if not s3_client:
        print(f"❌ Cannot upload {local_filepath}, S3 client not initialized.")
        return False
    if not bucket_name:
        print(f"❌ Cannot upload {local_filepath}, Bucket name is missing.")
        return False
    if not os.path.exists(local_filepath):
        print(f"❌ Local file not found for upload: {local_filepath}")
        return False

    try:
        print(f"⬆️ Uploading {local_filepath} to s3://{bucket_name}/{s3_key}...")
        s3_client.upload_file(local_filepath, bucket_name, s3_key)
        print(f"✅ Successfully uploaded to s3://{bucket_name}/{s3_key}")

        # Remove the local file only after successful upload
        try:
            os.remove(local_filepath)
            print(f"🗑️ Removed local file: {local_filepath}")
        except OSError as e:
            # Log warning but consider upload successful if removal fails
            print(
                f"⚠️ Warning: Failed to remove local file {local_filepath} after upload: {e}"
            )
        return True  # Indicate upload success

    except ClientError as e:
        # Provide more specific feedback for common S3 errors
        error_code = e.response.get("Error", {}).get("Code")
        if error_code == "NoSuchBucket":
            print(f"❌ S3 Error: Bucket '{bucket_name}' does not exist.")
        elif error_code == "AccessDenied":
            print(
                f"❌ S3 Error: Access Denied. Check permissions for uploading to s3://{bucket_name}/{s3_key}."
            )
        else:
            print(f"❌ S3 ClientError uploading {local_filepath}: {error_code} - {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error uploading {local_filepath}: {e}")
        return False


def get_google_places_api_key():
    # Load from credentials_maps.env
    load_dotenv(os.path.join(os.getcwd(), "credentials_maps.env"))
    return os.getenv("GOOGLE_PLACES_API_KEY")
