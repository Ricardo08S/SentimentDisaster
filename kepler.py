import pandas as pd
import geopandas as gpd
import glob
import os
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter

folder_path = './'
file_pattern = folder_path + 'filtered_processed_*.csv'
all_files = glob.glob(file_pattern)

print(f"Found {len(all_files)} files to process.")

dfs = []

for filepath in all_files:
    filename = os.path.basename(filepath)
    parts = filename.replace('.csv', '').split('_')
    
    if len(parts) >= 5:
        location_from_file = parts[2]
        disaster_type = parts[3]
        news_source = parts[4]
        
        try:
            df_temp = pd.read_csv(filepath)
            
            df_temp['location_name'] = location_from_file
            df_temp['disaster_type'] = disaster_type
            df_temp['news_source'] = news_source
                        
            dfs.append(df_temp)
            print(f"  > Merged: {location_from_file} | {disaster_type}")
            
        except Exception as e:
            print(f"  > Error reading {filename}: {e}")

if not dfs:
    print("❌ No valid files found. Please check your folder path.")
else:
    master_df = pd.concat(dfs, ignore_index=True)
    print(f"\n✅ Merged Dataset: {len(master_df)} total rows.")

    print("\nStarting Geocoding...")
    
    geolocator = Nominatim(user_agent="bali_disaster_thesis_final")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    
    unique_locations = master_df['location_name'].unique()
    location_cache = {}
    
    print(f"Geocoding {len(unique_locations)} unique locations...")
    
    for loc in unique_locations:
        try:
            location_data = geocode(f"{loc}, Bali, Indonesia")
            if location_data:
                location_cache[loc] = (location_data.latitude, location_data.longitude)
                print(f"  > Found: {loc} -> {location_data.latitude}, {location_data.longitude}")
            else:
                print(f"  > Not Found: {loc}")
                location_cache[loc] = (None, None)
        except:
            location_cache[loc] = (None, None)
            
    master_df['coordinates'] = master_df['location_name'].map(location_cache)
    master_df[['latitude', 'longitude']] = pd.DataFrame(master_df['coordinates'].tolist(), index=master_df.index)
    
    final_df = master_df.dropna(subset=['latitude', 'longitude'])
    
    print(f"\nCreating GeoJSON with {len(final_df)} points...")
    
    gdf = gpd.GeoDataFrame(
        final_df, geometry=gpd.points_from_xy(final_df.longitude, final_df.latitude)
    )
    
    output_path = folder_path + 'final_disaster_map.geojson'
    gdf.to_file(output_path, driver='GeoJSON')
    
    print(f"\nSUCCESS! Download your map file here: {output_path}")