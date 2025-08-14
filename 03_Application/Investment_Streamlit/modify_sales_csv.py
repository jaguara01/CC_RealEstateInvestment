import pandas as pd
import reverse_geocode

# 1) Load CSV
df = pd.read_csv("data/Barcelona_Sale.csv", low_memory=False)
df["FLATLOCATIONID"] = df["FLATLOCATIONID"].astype(str)

# 2) Unique lat/lon pairs (optional: dedupe if your data is big)
coords = list(zip(df["LATITUDE"], df["LONGITUDE"]))

# 3) Reverse geocode
locs = reverse_geocode.search(coords)

# 4) Show example of fields (to debug structure)
print("🔍 Sample location dict:", locs[0])

# 5) Safely format ADDRESS using available fields
def format_address(loc):
    city = loc.get("city", "")
    admin = loc.get("admin1", "") or loc.get("admin2", "") or ""
    return f"{city}, {admin}".strip(", ")

df["ADDRESS"] = [format_address(loc) for loc in locs]

# 6) Save to new CSV
df.to_csv("data/Barcelona_Sale_enriched.csv", index=False)
print("✅ Saved enriched CSV with 'ADDRESS' column.")
