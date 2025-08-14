import requests
import pandas as pd
import utils

overpass_url = "https://overpass-api.de/api/interpreter"

# Modified query with better agency tags and additional housing-related data
query = """
[out:json][timeout:90];
area[name="Barcelona"]->.searchArea;
(
  // Buildings and land use
  way["building"](area.searchArea);
  way["landuse"](area.searchArea);
  
  // Real estate agencies with multiple tag variations
  node[~"amenity|office|shop"~"real_estate_agent|real_estate|estate_agent"](area.searchArea);
  node["shop"="estate_agent"](area.searchArea);
  
  // Additional housing-related data
  node["amenity"="housing_office"](area.searchArea);
  node["office"="property_management"](area.searchArea);
  node["amenity"="public_building"]["government"="housing"](area.searchArea);
  node["building"="construction"](area.searchArea);
  node["shop"="rental"](area.searchArea);
);
out center;
"""

try:
    response = requests.post(overpass_url, data={'data': query})
    response.raise_for_status()
    data = response.json()
except Exception as e:
    print(f"Error: {e}")
    exit()

def get_coordinates(element):
    """Improved coordinate extraction with fallbacks"""
    if element['type'] == 'node':
        return (element.get('lat'), element.get('lon'))
    return (element.get('center', {}).get('lat'), element.get('center', {}).get('lon'))

# Initialize data containers
buildings = []
agencies = []
land_use = []
housing_offices = []
property_managers = []
rental_shops = []

for element in data['elements']:
    elem_type = element.get('type')
    tags = element.get('tags', {})
    lat, lon = get_coordinates(element)
    
    common = {
        'id': element.get('id'),
        'type': elem_type,
        'lat': lat,
        'lon': lon,
        'name': tags.get('name'),
        'address': tags.get('addr:full') or tags.get('addr:street'),
        'postcode': tags.get('addr:postcode')
    }
    
    # Buildings
    if 'building' in tags:
        buildings.append({
            **common,
            'building_type': tags.get('building'),
            'levels': tags.get('building:levels'),
            'year': tags.get('start_date')
        })
    
    # Land Use
    if 'landuse' in tags:
        land_use.append({
            **common,
            'landuse_type': tags.get('landuse'),
            'area_m2': tags.get('area')
        })
    
    # Real Estate Agencies
    if any(tags.get(key) in ['real_estate_agent', 'real_estate', 'estate_agent'] 
       for key in ['amenity', 'office', 'shop']):
        agencies.append({
            **common,
            'phone': tags.get('phone'),
            'website': tags.get('website'),
            'email': tags.get('email'),
            'company': tags.get('company')
        })
    
    # Housing Offices
    if tags.get('amenity') == 'housing_office':
        housing_offices.append({
            **common,
            'government': tags.get('government'),
            'service': tags.get('service')
        })
    
    # Property Managers
    if tags.get('office') == 'property_management':
        property_managers.append({
            **common,
            'managed_units': tags.get('managed_units'),
            'services': tags.get('services')
        })
    
    # Rental Shops
    if tags.get('shop') == 'rental':
        rental_shops.append({
            **common,
            'rental_type': tags.get('rental'),
            'furnished': tags.get('furnished')
        })

# Save all categories to separate files
def save_csv(data, filename):
    if data:
        pd.DataFrame(data).to_csv(filename, index=False, encoding='utf-8-sig')
        print(f"Created {filename} with {len(data)} entries")

local_filenames = ['buildings.csv', 'land_use.csv', 'real_estate_agencies.csv', 'property_managers.csv']

save_csv(buildings, local_filenames[0])
save_csv(land_use, local_filenames[1])
save_csv(agencies, local_filenames[2])
save_csv(property_managers, local_filenames[3])

print("Data extraction complete!")

if __name__ == "__main__":
    try:
        s3 = utils.initialize_s3()
        for local_filename in local_filenames:
            utils.upload_to_s3(s3, local_filename, f"amenities/openstreetmap/{local_filename}", "02-semistructured-data")
    except Exception as e:
        print(f"An error occurred: {e}")
