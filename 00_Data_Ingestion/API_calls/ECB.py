import requests
import utils
import os

def download_ecb_csv(dataset, series_key, local_filename):
    """
    Downloads a CSV file from the ECB SDMX RESTful API using dataset and series key.
    """
    url = f"https://data-api.ecb.europa.eu/service/data/{dataset}/{series_key}?format=csvdata"
    response = requests.get(url)
    response.raise_for_status()
    
    with open(local_filename, "wb") as f:
        f.write(response.content)
    print(f"Downloaded and saved: {local_filename}")

if __name__ == "__main__":
    series_map = {
        # ICP - Inflation
        ("ICP", "M.U2.N.041000.4.INX"): "hicp_actual_rentals_housing.csv",

        #MIR - Bank interest rates
        ("MIR", "M.U2.B.A2C.A.R.A.2250.EUR.N"): "bank_interest_rates_house_purchase.csv",
        ("MIR", "M.U2.B.A2C.AM.R.A.2250.EUR.N"): "cost_of_borrowing_house_purchase.csv",
        ("MIR", "M.U2.B.A2C.P.R.A.2250.EUR.N"): "bank_rates_irf_over_10y.csv",
        ("MIR", "M.U2.B.A2C.F.R.A.2250.EUR.N"): "bank_rates_floating_irf_up_to_1y.csv",
        ("MIR", "M.U2.B.A2Z.A.B.A.2250.EUR.N"): "bank_business_volumes_credit_cards.csv"
    }

    s3 = utils.initialize_s3()

    for (dataset, series_key), filename in series_map.items():
        try:
            # Descargar CSV
            download_ecb_csv(dataset, series_key, filename)
            # Subir a S3
            s3_path = f"financial-data/ecb/{filename}"
            utils.upload_to_s3(s3, filename, s3_path, bucket_name="01-structured-data")
        except Exception as e:
            print(f"Error with {series_key}: {e}")
