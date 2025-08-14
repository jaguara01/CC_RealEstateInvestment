import requests
from urllib.parse import urlparse
from utils import (
    initialize_s3,
    upload_to_s3,
)
import os  # Needed for os.remove potentially called by utils.upload_to_s3

# --- Import bucket name from central config ---
from src.config import AWS_BUCKET_NAME

# ---------------------------------------------

# Define datasets with their sections, dataset IDs, and resource filenames
datasets = [
    # Amenities (formerly City and services)
    {
        "section": "amenities",
        "dataset_id": "presencia-absencia-vegetacio",
        "resource_names": ["2017_vegetacio.gpkg", "2015_vulnera_faltaverd.gpkg"],
    },
    {
        "section": "amenities",
        "dataset_id": "intensitat-activitat-turistica",
        "resource_names": [
            "2019_turisme_allotjament.gpkg",
            "2017_turisme_atractius.gpkg",
            "2016_turisme_oci.gpkg",
        ],
    },
    {
        "section": "amenities",
        "dataset_id": "espais-verds-publics",
        "resource_names": ["2019_pev_parcs_od.gpkg"],
    },
    {
        "section": "amenities",
        "dataset_id": "manteniment-lloguer",
        "resource_names": [
            "2017_hab_od.gpkg",
            "2017-2016_hab_dif_od.gpkg",
            "2017_taxa_lloguer_od.gpkg",
            "2016_hab_od.gpkg",
        ],
    },
    {
        "section": "amenities",
        "dataset_id": "np-espais-gimnastica",
        "resource_names": ["opendatabcn_NP-NASIA_Espais-fer-gimnastica-csv.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "culturailleure-bibliotequesimuseus",
        "resource_names": ["opendatabcn_cultura_biblioteques-i-museus.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "transports",
        "resource_names": ["Transports.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "estacions-bus",
        "resource_names": ["ESTACIONS_BUS.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "punts-informacio-turistica",
        "resource_names": ["opendatabcn_pics-csv.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "equipament-transports-i-serveis-relacionats",
        "resource_names": [
            "opendatabcn_llista-equipament-transports-i-serveis-relacionats-csv.csv"
        ],
    },
    {
        "section": "amenities",
        "dataset_id": "habitatges-us-turistic",
        "resource_names": ["hut_comunicacio_opendata.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "equipaments-culturals-icub",
        "resource_names": ["Equipaments_del_mapa.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "esports-instal-lacions-esportives",
        "resource_names": ["opendatabcn_esports_instalacions-esportives.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "culturailleure-espaismusicacopes",
        "resource_names": ["opendatabcn_cultura_espais-de-musica-i-copes.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "culturailleure-cinemesteatresauditoris",
        "resource_names": ["opendatabcn_cultura_cines-teatres-auditoris.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "allotjaments-altres",
        "resource_names": ["opendatabcn_allotjament_altres-allotjaments-csv.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "mediambient-puntsverdsbarri",
        "resource_names": [
            "opendatabcn_medi-ambient_medi-ambient_punt-verd-de-barri.csv"
        ],
    },
    {
        "section": "amenities",
        "dataset_id": "exclusio-residencial",
        "resource_names": ["2018-2017_hab_exclusio_od.gpkg"],
    },
    {
        "section": "amenities",
        "dataset_id": "arbrat-zona",
        "resource_names": ["OD_Arbrat_Zona_BCN.csv"],
    },
    {
        "section": "amenities",
        "dataset_id": "zbe-ambit",
        "resource_names": ["2023_Ambit_ZBE_Barcelona.gpkg"],
    },
    # Financial data
    {
        "section": "financial-data",
        "dataset_id": "h2mave-totalt4b",
        "resource_names": ["h2mavetotalt4b.csv"],
    },
    {
        "section": "financial-data",
        "dataset_id": "h2mave-anualt1b",
        "resource_names": ["h2maveanualt1b.csv"],
    },
    {
        "section": "financial-data",
        "dataset_id": "habitatges-2na-ma",
        "resource_names": ["2015_HABITATGES_2NA_MA.csv"],
    },
    {
        "section": "financial-data",
        "dataset_id": "est-cadastre-habitatges-valor-cadastral",
        "resource_names": ["2025_Loc_hab_valors.csv"],
    },
    # Regulatory data
    {
        "section": "regulatory-data",
        "dataset_id": "est-cadastre-carrecs-quota-cadastral",
        "resource_names": ["2025_Carrecs_quota.csv"],
    },
    {
        "section": "regulatory-data",
        "dataset_id": "est-cadastre-habitatges-propietari",
        "resource_names": ["2025_Loc_hab_tipus_propietari.csv"],
    },
    {
        "section": "regulatory-data",
        "dataset_id": "est-cadastre-carrecs-tipus-propietari",
        "resource_names": ["2025_Carrec_tipus_propietari.csv"],
    },
    # Structural and geographic data
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-edificacions-edat-mitjana",
        "resource_names": ["2025_Edificacions_edat_mitjana.csv"],
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-habitatges-edat-mitjana",
        "resource_names": ["2025_Loc_hab_edat_mitjana.csv"],
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-habitatges-superficie",
        "resource_names": ["2025_Loc_hab_sup.csv"],
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-edificacions-nombre-locals",
        "resource_names": ["2025_Edificacions_nombre_locals.csv"],
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-edificacions-superficie",
        "resource_names": ["2025_Edificacions_superficie.csv"],
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-finques-superficie",
        "resource_names": ["2025_Finques_superficie.csv"],
    },
]

# Section to S3 path mapping
SECTION_PATHS = {
    "amenities": "amenities/ajuntament-barcelona",
    "financial-data": "financial-data/ajuntament-barcelona",
    "regulatory-data": "regulatory-data/ajuntament-barcelona",
    "structural-geographic-data": "structural-geographic-data/ajuntament-barcelona",
}


def download_file(url, local_filename):
    """Downloads a file from the provided URL and saves it locally."""
    # Consider adding User-Agent header for external requests
    headers = {"User-Agent": "MyDataPipelineBot/1.0"}
    print(f"Downloading {local_filename} from {url}...")
    response = requests.get(
        url, headers=headers, stream=True
    )  # Use stream=True for potentially large files
    response.raise_for_status()  # Check for HTTP errors

    # Save streamed content to avoid loading large files entirely into memory
    with open(local_filename, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    print(f"File downloaded and saved locally as {local_filename}")


def get_resource_filename(resource):
    """Extract filename from resource's 'name' field or URL as fallback."""
    # Prioritize 'name' field, clean special characters if necessary
    # Be careful with cleaning - ensure it doesn't create duplicate filenames unintentionally
    name = resource.get(
        "name", ""
    ).strip()  # Keep original name for matching if possible
    if name and name in dataset.get(
        "resource_names", []
    ):  # Check if original name matches expected
        # Optionally add cleaning here ONLY if necessary for S3 key, e.g.
        # clean_name = name.replace(' ', '_').replace('/', '-')
        # return clean_name
        return name  # Return original name if it matches directly

    # Fallback: Use specified name from datasets list if available
    # This assumes resource_names contains the desired final filename
    if dataset.get("resource_names"):
        # Try matching by URL or other metadata if 'name' doesn't match expected
        # For simplicity now, we might just use the first expected name if logic gets complex
        pass  # Placeholder - needs more robust logic if API 'name' field is unreliable

    # Ultimate Fallback to URL parsing if 'name' is unreliable/missing
    url = resource.get("url", "")
    parsed_name = urlparse(url).path.split("/")[-1]
    if not parsed_name:  # Handle edge case of URL ending in /
        print(f"Warning: Could not determine filename for resource URL: {url}")
        return None  # Or generate a unique name
    return parsed_name


if __name__ == "__main__":
    # --- Check if bucket name loaded correctly ---
    if not AWS_BUCKET_NAME:
        print(
            "❌ Error: AWS_BUCKET_NAME not found in environment/config. Cannot upload to S3."
        )
        exit(1)
    # ---------------------------------------------

    s3 = initialize_s3()
    if not s3:
        print("Exiting due to S3 initialization failure.")
        exit(1)  # Exit if S3 client fails

    # --- Use bucket name from config ---
    # REMOVED: bucket_for_upload = "01-structured-data"
    # ----------------------------------

    for (
        dataset
    ) in datasets:  # Make dataset accessible in get_resource_filename if needed
        section = dataset["section"]
        dataset_id = dataset["dataset_id"]
        resource_names = dataset.get("resource_names", [])  # Use .get for safety

        print(f"\nProcessing dataset: {dataset_id} (Section: {section})")

        try:
            # Retrieve package metadata
            package_url = f"https://opendata-ajuntament.barcelona.cat/data/api/action/package_show?id={dataset_id}"
            response = requests.get(package_url)
            response.raise_for_status()
            data = response.json()

            if not data.get("success"):
                print(
                    f"Error: API request for dataset {dataset_id} failed. Response: {data.get('error', 'Unknown error')}"
                )
                continue

            result_data = data.get("result")
            if not result_data:
                print(
                    f"Error: No 'result' field found in API response for {dataset_id}."
                )
                continue

            resources = result_data.get("resources", [])
            if not resources:
                print(f"No resources found for dataset {dataset_id}.")
                continue

            # Refined matching logic - handle potential None filenames
            matched_resources = []
            available_api_filenames = []
            resource_dict = {}  # Store resource by filename for easier lookup

            for resource in resources:
                filename = get_resource_filename(resource)
                if filename:  # Only process if a filename could be determined
                    available_api_filenames.append(filename)
                    resource_dict[filename] = resource["url"]

            # --- Tier 1: Exact matches in resource_names ---
            for expected_name in resource_names:
                if expected_name in resource_dict:
                    matched_resources.append(
                        (expected_name, resource_dict[expected_name])
                    )

            # --- Tier 2: CSV fallback (if no exact matches found yet) ---
            if (
                not matched_resources and resource_names
            ):  # Check CSVs only if specific names were expected
                print(
                    f"No exact matches for {resource_names}. Checking CSVs... (Available: {available_api_filenames})"
                )
                for filename, url in resource_dict.items():
                    if filename.lower().endswith(".csv"):
                        matched_resources.append((filename, url))

            # --- Tier 3: Any file type (if still no matches or no specific names required) ---
            if not matched_resources:
                print_prefix = (
                    "No specific or CSV matches found."
                    if resource_names
                    else "No specific resources listed."
                )
                print(
                    f"{print_prefix} Taking any available files... (Available: {available_api_filenames})"
                )
                # Convert dict items back to list of tuples
                matched_resources = list(resource_dict.items())

            # Limit to max 3 files
            matched_resources = matched_resources[
                :3
            ]  # Limit *after* finding potential matches
            if matched_resources:
                print(
                    f"Found {len(matched_resources)} files to process: {[name for name, url in matched_resources]}"
                )
            else:
                print(f"No suitable files found to process for {dataset_id}.")
                continue  # Skip to next dataset if no files found/matched

            # Download & upload loop
            for filename, url in matched_resources:
                local_temp_filename = f"temp_{filename}"  # Use a temporary local name
                try:
                    download_file(url, local_temp_filename)
                    s3_key = f"{SECTION_PATHS[section]}/{filename}"  # Use original intended filename for S3 key
                    # --- Use bucket name from config ---
                    success = upload_to_s3(
                        s3, local_temp_filename, s3_key, AWS_BUCKET_NAME
                    )
                    # ------------------------------------
                    if not success:
                        print(
                            f"⚠️ Upload failed for {filename} (local: {local_temp_filename})"
                        )
                    # upload_to_s3 now handles printing success and removal

                except requests.exceptions.RequestException as e:
                    print(f"Failed to download {filename}: {str(e)}")
                except Exception as e:
                    print(
                        f"Failed processing {filename} (local: {local_temp_filename}): {str(e)}"
                    )

        except requests.exceptions.RequestException as e:
            print(f"HTTP Error retrieving metadata for {dataset_id}: {str(e)}")
        except Exception as e:
            print(f"Critical error processing dataset {dataset_id}: {str(e)}")
