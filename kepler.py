import pandas as pd
import geopandas as gpd
import glob
import os
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
import spacy

# -----------------------------
# 1. Load NER model
# -----------------------------
nlp = spacy.load("xx_ent_wiki_sm")

# -----------------------------
# 2. Fallback coordinates (edit this!)
# -----------------------------
fallback_coords = {
    "badung": (-8.5819, 115.1770),
    "bangli": (-8.4543, 115.3540),
    "gianyar": (-8.5445, 115.3250),
    "tabanan": (-8.5440, 115.1250),
    "denpasar": (-8.6705, 115.2126),
    "klungkung": (-8.5449, 115.4040),
    "karangasem": (-8.4286, 115.5700),
    "jembrana": (-8.3589, 114.6400),
    "buleleng": (-8.1120, 115.0900),
}

# -----------------------------
# 3. Function: extract location names from text
# -----------------------------
def extract_locations(text):
    if not isinstance(text, str):
        return []
    doc = nlp(text)
    locs = [ent.text for ent in doc.ents if ent.label_ in ["GPE", "LOC"]]
    return list(set(locs))  # unique only


# -----------------------------
# START ORIGINAL CODE
# -----------------------------
folder_path = './unified'
file_pattern = os.path.join(folder_path, 'processed_*.csv')
print(f"Using folder_path={folder_path}, file_pattern={file_pattern}")

if not os.path.isdir(folder_path):
    print(f"Folder not found: {folder_path}")
    all_files = []
else:
    all_files = glob.glob(file_pattern)

print(f"Found {len(all_files)} files to process.")
dfs = []

# -------- read all CSVs --------
for filepath in all_files:
    filename = os.path.basename(filepath)
    parts = filename.replace('.csv', '').split('_')

    disasters = {'badai', 'banjir', 'erupsi', 'gempa', 'longsor'}
    disaster_idx = next((i for i, p in enumerate(parts) if p.lower() in disasters), None)

    if disaster_idx is not None and disaster_idx >= 1:
        disaster_type = parts[disaster_idx]
        location_from_file = parts[disaster_idx - 1]
        news_source = None
        if disaster_idx + 1 < len(parts):
            candidate = parts[disaster_idx + 1]
            if candidate.lower() != 'valid':
                news_source = candidate
    else:
        if len(parts) >= 4:
            location_from_file = parts[1]
            disaster_type = parts[2]
            news_source = parts[3] if len(parts) > 3 else None
        else:
            print(f"Skipping (unrecognized filename): {filename}")
            continue

    try:
        df_temp = pd.read_csv(filepath)
        df_temp['location_name'] = location_from_file
        df_temp['disaster_type'] = disaster_type
        df_temp['news_source'] = news_source
        dfs.append(df_temp)
        print(f"  > Merged: {location_from_file} | {disaster_type} | {news_source}")

    except Exception as e:
        print(f"  > Error reading {filename}: {e}")

# -------- combine all files --------
master_df = pd.concat(dfs, ignore_index=True)
print(f"\nMerged Dataset: {len(master_df)} rows")

# -----------------------------
# 4. Extract all location mentions per article
# -----------------------------
master_df["all_locations"] = master_df.apply(
    lambda row: extract_locations(str(row.get("title", "")) + " " +
                                  str(row.get("cleaned_content", ""))),
    axis=1
)

print("Finished location extraction!")
from tqdm import tqdm

# -----------------------------
# 5. Setup geocoder
# -----------------------------
geolocator = Nominatim(user_agent="bali_disaster_thesis_final")
geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

# -----------------------------
# 6. Convert each extracted location into coordinates
# -----------------------------
records = []

# Initialize tqdm progress bar
print("Geocoding each article and its detected locations...\n")
for idx, row in tqdm(master_df.iterrows(), total=len(master_df), desc="Progress"):
    base_region = row["location_name"].lower()

    if len(row["all_locations"]) == 0:
        # fallback to kabupaten center
        lat, lon = fallback_coords.get(base_region, (None, None))
        if lat is not None:
            records.append({
                "title": row.get("title"),
                "disaster_type": row["disaster_type"],
                "location_detected": base_region,
                "latitude": lat,
                "longitude": lon
            })
        continue

    # else: geocode each detected location
    for loc in row["all_locations"]:
        query = f"{loc}, {base_region}, Bali, Indonesia"
        try:
            result = geocode(query)
            if result:
                records.append({
                    "title": row.get("title"),
                    "disaster_type": row["disaster_type"],
                    "location_detected": loc,
                    "latitude": result.latitude,
                    "longitude": result.longitude
                })
            else:
                # fallback when geocoder fails
                lat, lon = fallback_coords.get(base_region, (None, None))
                if lat:
                    records.append({
                        "title": row.get("title"),
                        "disaster_type": row["disaster_type"],
                        "location_detected": loc,
                        "latitude": lat,
                        "longitude": lon
                    })
        except:
            pass

# -----------------------------
# 7. Convert to GeoDataFrame
# -----------------------------
final_df = pd.DataFrame(records)
gdf = gpd.GeoDataFrame(
    final_df,
    geometry=gpd.points_from_xy(final_df.longitude, final_df.latitude)
)

output_path = os.path.join(folder_path, "unified_map.geojson")
gdf.to_file(output_path, driver="GeoJSON")

print("\nSUCCESS! Created GeoJSON with multiple coordinates per article:")
print(output_path)