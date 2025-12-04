import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
from location_extractor import extract_locations
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import time

# --------------------------
# INITIAL SETUP
# --------------------------

INPUT_FILE = "labeled_sentiment_data_unified.csv"
CSV_OUTPUT = "geocoded_disasters.csv"
GEOJSON_OUTPUT = "final_disaster_map.geojson"

print("📌 Loading unified dataset...")
df = pd.read_csv(INPUT_FILE)
print(f"Loaded {len(df)} rows.")

# Required for your dataset
required_cols = ["cleaned_content", "sentiment_label", "keyword"]
for col in required_cols:
    if col not in df.columns:
        raise ValueError(f"❌ Missing column: {col}")

# --------------------------
# GEOCODING SETUP
# --------------------------

geolocator = Nominatim(user_agent="bali-disaster-geocoder")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

def geocode_location(location_name):
    """Try geocoding with Bali context."""
    try:
        result = geocode(f"{location_name}, Bali, Indonesia")
        if result:
            return result.latitude, result.longitude, location_name
    except:
        pass
    return None, None, None

# --------------------------
# LOCATION SELECTION LOGIC
# --------------------------

def choose_best_location(row):
    """
    1. Use detected article location
    2. Else use kabupaten name from keyword column (2nd word)
    3. Else fallback to Bali
    """
    # (1) Extract article-based locations
    article_locs = extract_locations(row["cleaned_content"])

    if article_locs:
        return article_locs[0]

    # (2) Keyword fallback
    kw = str(row["keyword"]).strip().split()
    if len(kw) >= 2:
        kabupaten = kw[1]          # second word
        return kabupaten

    # (3) Hard fallback (should rarely happen)
    return "Bali"

# --------------------------
# PROCESS EACH ROW
# --------------------------

lat_list, lon_list, detected_list = [], [], []

for idx, row in df.iterrows():
    print(f"\n🔍 Row {idx}/{len(df)}")

    best_loc = choose_best_location(row)
    print(f"📌 Final chosen location: {best_loc}")

    lat, lon, final_loc = geocode_location(best_loc)

    lat_list.append(lat)
    lon_list.append(lon)
    detected_list.append(final_loc)

# --------------------------
# MERGE RESULTS BACK
# --------------------------

df["detected_location"] = detected_list
df["lat"] = lat_list
df["lon"] = lon_list

df.to_csv(CSV_OUTPUT, index=False)
print(f"\n✅ Saved geocoded CSV → {CSV_OUTPUT}")

# --------------------------
# CREATE GEOJSON
# --------------------------

filtered = df.dropna(subset=["lat", "lon"])

gdf = gpd.GeoDataFrame(
    filtered,
    geometry=[Point(xy) for xy in zip(filtered.lon, filtered.lat)],
    crs="EPSG:4326"
)

properties_to_keep = [
    "sentiment_label",
    "cleaned_content",
    "keyword",
    "detected_location",
] + [col for col in df.columns if col not in ["lat", "lon", "geometry"]]

gdf[properties_to_keep + ["geometry"]].to_file(GEOJSON_OUTPUT, driver="GeoJSON")

print(f"🌍 GeoJSON exported → {GEOJSON_OUTPUT}")
print("🎉 All done!")
