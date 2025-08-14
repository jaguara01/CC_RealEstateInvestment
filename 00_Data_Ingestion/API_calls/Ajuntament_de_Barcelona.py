import requests
from urllib.parse import urlparse
import utils

# Define datasets with their sections, dataset IDs, and resource filenames
datasets = [
    # Amenities (formerly City and services)
    {
        "section": "amenities",
        "dataset_id": "presencia-absencia-vegetacio",
        "resource_names": ["2017_vegetacio.gpkg", "2015_vulnera_faltaverd.gpkg"]
    },
    {
        "section": "amenities",
        "dataset_id": "intensitat-activitat-turistica",
        "resource_names": ["2019_turisme_allotjament.gpkg", "2017_turisme_atractius.gpkg", "2016_turisme_oci.gpkg"]
    },
    {
        "section": "amenities",
        "dataset_id": "espais-verds-publics",
        "resource_names": ["2019_pev_parcs_od.gpkg"]
    },
    {
        "section": "amenities",
        "dataset_id": "manteniment-lloguer",
        "resource_names": ["2017_hab_od.gpkg", "2017-2016_hab_dif_od.gpkg", "2017_taxa_lloguer_od.gpkg", "2016_hab_od.gpkg"]
    },
    {
        "section": "amenities",
        "dataset_id": "np-espais-gimnastica",
        "resource_names": ["opendatabcn_NP-NASIA_Espais-fer-gimnastica-csv.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "culturailleure-bibliotequesimuseus",
        "resource_names": ["opendatabcn_cultura_biblioteques-i-museus.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "transports",
        "resource_names": ["Transports.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "estacions-bus",
        "resource_names": ["ESTACIONS_BUS.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "punts-informacio-turistica",
        "resource_names": ["opendatabcn_pics-csv.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "equipament-transports-i-serveis-relacionats",
        "resource_names": ["opendatabcn_llista-equipament-transports-i-serveis-relacionats-csv.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "habitatges-us-turistic",
        "resource_names": ["hut_comunicacio_opendata.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "equipaments-culturals-icub",
        "resource_names": ["Equipaments_del_mapa.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "esports-instal-lacions-esportives",
        "resource_names": ["opendatabcn_esports_instalacions-esportives.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "culturailleure-espaismusicacopes",
        "resource_names": ["opendatabcn_cultura_espais-de-musica-i-copes.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "culturailleure-cinemesteatresauditoris",
        "resource_names": ["opendatabcn_cultura_cines-teatres-auditoris.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "allotjaments-altres",
        "resource_names": ["opendatabcn_allotjament_altres-allotjaments-csv.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "mediambient-puntsverdsbarri",
        "resource_names": ["opendatabcn_medi-ambient_medi-ambient_punt-verd-de-barri.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "exclusio-residencial",
        "resource_names": ["2018-2017_hab_exclusio_od.gpkg"]
    },
    {
        "section": "amenities",
        "dataset_id": "arbrat-zona",
        "resource_names": ["OD_Arbrat_Zona_BCN.csv"]
    },
    {
        "section": "amenities",
        "dataset_id": "zbe-ambit",
        "resource_names": ["2023_Ambit_ZBE_Barcelona.gpkg"]
    },

    # Financial data
    {
        "section": "financial-data",
        "dataset_id": "h2mave-totalt4b",
        "resource_names": ["h2mavetotalt4b.csv"]
    },
    {
        "section": "financial-data",
        "dataset_id": "h2mave-anualt1b",
        "resource_names": ["h2maveanualt1b.csv"]
    },
    {
        "section": "financial-data",
        "dataset_id": "habitatges-2na-ma",
        "resource_names": ["2015_HABITATGES_2NA_MA.csv"]
    },
    {
        "section": "financial-data",
        "dataset_id": "est-cadastre-habitatges-valor-cadastral",
        "resource_names": ["2025_Loc_hab_valors.csv"]
    },

    # Regulatory data
    {
        "section": "regulatory-data",
        "dataset_id": "est-cadastre-carrecs-quota-cadastral",
        "resource_names": ["2025_Carrecs_quota.csv"]
    },
    {
        "section": "regulatory-data",
        "dataset_id": "est-cadastre-habitatges-propietari",
        "resource_names": ["2025_Loc_hab_tipus_propietari.csv"]
    },
    {
        "section": "regulatory-data",
        "dataset_id": "est-cadastre-carrecs-tipus-propietari",
        "resource_names": ["2025_Carrec_tipus_propietari.csv"]
    },

    # Structural and geographic data
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-edificacions-edat-mitjana",
        "resource_names": ["2025_Edificacions_edat_mitjana.csv"]
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-habitatges-edat-mitjana",
        "resource_names": ["2025_Loc_hab_edat_mitjana.csv"]
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-habitatges-superficie",
        "resource_names": ["2025_Loc_hab_sup.csv"]
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-edificacions-nombre-locals",
        "resource_names": ["2025_Edificacions_nombre_locals.csv"]
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-edificacions-superficie",
        "resource_names": ["2025_Edificacions_superficie.csv"]
    },
    {
        "section": "structural-geographic-data",
        "dataset_id": "est-cadastre-finques-superficie",
        "resource_names": ["2025_Finques_superficie.csv"]
    }
]

# Section to S3 path mapping
SECTION_PATHS = {
    "amenities": "amenities/ajuntament-barcelona",
    "financial-data": "financial-data/ajuntament-barcelona",
    "regulatory-data": "regulatory-data/ajuntament-barcelona",
    "structural-geographic-data": "structural-geographic-data/ajuntament-barcelona"
}

def download_file(url, local_filename):
    """Downloads a file from the provided URL and saves it locally."""
    response = requests.get(url)
    response.raise_for_status()
    
    with open(local_filename, "wb") as f:
        f.write(response.content)
    print(f"File downloaded and saved as {local_filename}")

def get_resource_filename(resource):
    """Extract filename from resource's 'name' field or URL as fallback."""
    # Prioritize 'name' field, clean special characters
    name = resource.get('name', '').strip().replace(' ', '_')
    if name:
        return name
    # Fallback to URL parsing if 'name' is empty
    url = resource.get('url', '')
    return urlparse(url).path.split('/')[-1]

if __name__ == "__main__":
    s3 = utils.initialize_s3()
    
    for dataset in datasets:
        section = dataset["section"]
        dataset_id = dataset["dataset_id"]
        resource_names = dataset["resource_names"]
        
        print(f"\nProcessing dataset: {dataset_id} (Section: {section})")
        
        try:
            # Retrieve package metadata
            package_url = f"https://opendata-ajuntament.barcelona.cat/data/api/action/package_show?id={dataset_id}"
            response = requests.get(package_url)
            response.raise_for_status()
            data = response.json()
            
            if not data.get("success"):
                print(f"Error: API request for dataset {dataset_id} failed.")
                continue

            resources = data["result"].get("resources", [])
            if not resources:
                print(f"No resources found for dataset {dataset_id}.")
                continue

            matched_resources = []
            debug_filenames = []

            # --- Tier 1: Exact matches ---
            for resource in resources:
                filename = get_resource_filename(resource)
                debug_filenames.append(filename)
                
                if filename in resource_names:
                    matched_resources.append((filename, resource['url']))
            
            # --- Tier 2: CSV fallback ---
            if not matched_resources:
                print(f"No explicit matches. Checking CSVs... (Available: {debug_filenames})")
                for resource in resources:
                    filename = get_resource_filename(resource)
                    if filename.lower().endswith('.csv'):
                        matched_resources.append((filename, resource['url']))
            
            # --- Tier 3: Any file type ---
            if not matched_resources:
                print(f"No CSVs found. Taking any file... (Available: {debug_filenames})")
                matched_resources = [(get_resource_filename(r), r['url']) for r in resources]
            
            # Limit to max 3 files
            matched_resources = matched_resources[:3]
            print(f"Found {len(matched_resources)} files to process")
            
            # Download & upload
            for filename, url in matched_resources:
                try:
                    download_file(url, filename)
                    s3_key = f"{SECTION_PATHS[section]}/{filename}"
                    utils.upload_to_s3(s3, filename, s3_key, "01-structured-data")
                    print(f"Uploaded {filename}")
                except Exception as e:
                    print(f"Failed to process {filename}: {str(e)}")
            
        except Exception as e:
            print(f"Critical error with {dataset_id}: {str(e)}")