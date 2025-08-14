import requests
import utils
import os
import json

def download_and_save_json(table_id, local_filename):
    """
    Downloads raw JSON data from INE Tempus API and saves it locally.
    """
    url = f"https://servicios.ine.es/wstempus/js/ES/DATOS_TABLA/{table_id}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    with open(local_filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"Saved JSON file: {local_filename}")

if __name__ == "__main__":
    # Mapping output filenames to INE table IDs
    datasets = {
        "urban_lease_decrees_by_type.json": "29276",
        "urban_lease_judgments_by_dispute.json": "49095",
        "urban_lease_judgments_by_ruling.json": "29275"
    }

    s3 = utils.initialize_s3()

    for filename, table_id in datasets.items():
        try:
            download_and_save_json(table_id, filename)

            #Upload to S3 bucket for semi-structured data
            s3_key = f"INE/{filename}"
            utils.upload_to_s3(s3, filename, s3_key, bucket_name="02-semistructured-data")

        except Exception as e:
            print(f"Error with {filename}: {e}")
