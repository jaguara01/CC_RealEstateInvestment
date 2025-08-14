import requests
import utils
import os

def download_ecb_csv(series_key, local_filename):
    """
    Downloads a CSV file from the ECB SDMX RESTful API using a dataset and series path.
    """
    dataset = "ICP"
    url = f"https://data-api.ecb.europa.eu/service/data/{dataset}/{series_key}?format=csvdata"
    response = requests.get(url)
    response.raise_for_status()
    
    with open(local_filename, "wb") as f:
        f.write(response.content)
    print(f"ECB CSV file downloaded and saved as {local_filename}")

if __name__ == "__main__":
    # ECB HICP - Actual rentals for housing
    series_key = "M.U2.N.041000.4.INX"
    local_filename = "hicp_actual_rentals.csv"

    try:
        # Download and upload to S3
        download_ecb_csv(series_key, local_filename)
        s3 = utils.initialize_s3()
        s3_key = f"financial-data/ecb/{local_filename}"
        utils.upload_to_s3(s3, local_filename, s3_key, bucket_name="01-structured-data")

    except Exception as e:
        print(f"An error occurred: {e}")