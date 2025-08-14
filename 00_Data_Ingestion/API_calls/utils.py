import os
from dotenv import load_dotenv

def initialize_s3():
    import boto3
    from dotenv import load_dotenv
    # Load environment variables from .aws/credentials.env
    load_dotenv(os.path.join(os.getcwd(), 'credentials.env'))  # Adjust the path if necessary

    # Get credentials from environment variables
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION", "us-east-1")  # Default region if not set

    # Initialize S3 client with credentials
    s3 = boto3.client(
        "s3",
        aws_access_key_id=aws_access_key_id,
        aws_secret_access_key=aws_secret_access_key,
        region_name=aws_region
    )
    return s3

def upload_to_s3(s3, local_filename, s3_key, bucket_name="bdm-project"):
    """
    Uploads the local file to the specified Amazon S3 bucket.
    """
    s3.upload_file(local_filename, bucket_name, s3_key)
    print(f"File {local_filename} uploaded to s3://{bucket_name}/{s3_key}")
    os.remove(local_filename)
    print("Local file removed after successful upload.")

def get_google_places_api_key():
    # Load from credentials_maps.env
    load_dotenv(os.path.join(os.getcwd(), 'credentials_maps.env'))
    return os.getenv("GOOGLE_PLACES_API_KEY")