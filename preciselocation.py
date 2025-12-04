#!/usr/bin/env python3
"""
Refine coordinates in a geojson by extracting place words from article text,
geocoding the extracted place (with kabupaten context), and falling back to kabupaten.
Input: unified_map_with_sentiment.geojson
Output: unified_map_refined.geojson (+ optional CSV backup)
"""

import json
import time
import math
from pathlib import Path
from geopy.geocoders import Nominatim
from geopy.extra.rate_limiter import RateLimiter
from location_extractor import extract_locations  # your extractor
from collections import defaultdict
from tqdm import tqdm

INPUT = "./geojson/unified_map_with_sentiment.geojson"
OUTPUT = "./geojson/unified_map_refined.geojson"
CSV_BACKUP = "./geojson/unified_map_refined.csv"  # optional

# ---- helper functions ----
def haversine(lon1, lat1, lon2, lat2):
    """Return distance in meters between two lon/lat points."""
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2.0)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2.0)**2
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1-a))

# ---- geocoder setup with caching ----
geolocator = Nominatim(user_agent="your_app_name_bali_disaster_refiner_v1")
rate_limited_geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)

geocode_cache = {}  # query -> (lat, lon, display_name)

def geocode_cached(query):
    """Geocode with caching. Returns (lat, lon, display_name) or (None,None,None)."""
    q = query.strip()
    if not q:
        return None, None, None
    if q in geocode_cache:
        return geocode_cache[q]
    try:
        loc = rate_limited_geocode(q)
        if loc:
            res = (float(loc.latitude), float(loc.longitude), loc.address)
            geocode_cache[q] = res
            return res
    except Exception as e:
        # print("Geocode error for", q, ":", e)
        pass
    geocode_cache[q] = (None, None, None)
    return (None, None, None)

# ---- load geojson ----
p = Path(INPUT)
if not p.exists():
    raise SystemExit(f"Input file not found: {INPUT}")

with p.open(encoding="utf-8") as f:
    data = json.load(f)

features = data.get("features", [])
print(f"Loaded {len(features)} features from {INPUT}")

# ---- process each feature ----
updated = 0
skipped = 0
errors = 0

# Useful to avoid re-geocoding identical candidate+kabupaten queries
tried_queries = set()

# Optional: gather rows for CSV backup
csv_rows = []

for i, feature in enumerate(tqdm(features, desc="Refining locations")):
    try:
        props = feature.get("properties", {})
        cleaned = props.get("cleaned_content") or props.get("content") or ""
        # Get existing kabupaten fallback: try properties.location_name or keyword (2nd word)
        kabupaten = None

        # try a few places where kabupaten might exist
        if props.get("location_name"):
            kabupaten = str(props.get("location_name")).strip()
        elif props.get("keyword"):
            kw = str(props.get("keyword")).strip().split()
            if len(kw) >= 2:
                kabupaten = kw[1]
        elif props.get("Location"):
            kabupaten = str(props.get("Location")).strip()

        # Normalize kabupaten if present
        if kabupaten:
            kabupaten = kabupaten.replace(",", "").strip()
        else:
            kabupaten = None

        # Try extracting place names from article text
        candidates = []
        if cleaned and cleaned.strip():
            try:
                extracted = extract_locations(cleaned)
                # ensure unique and preserve order
                seen = set()
                for ex in extracted:
                    exs = str(ex).strip()
                    if exs and exs.lower() not in seen:
                        seen.add(exs.lower())
                        candidates.append(exs)
            except Exception as e:
                # if your extractor fails, continue to fallback
                # print("location_extractor failed on feature", i, ":", e)
                candidates = []

        # Build list of query attempts in priority order
        query_attempts = []

        # 1) each candidate with kabupaten context: "candidate, kabupaten, Bali, Indonesia"
        if kabupaten:
            for c in candidates:
                query_attempts.append(f"{c}, {kabupaten}, Bali, Indonesia")

        # 2) each candidate with Bali context only: "candidate, Bali, Indonesia"
        for c in candidates:
            query_attempts.append(f"{c}, Bali, Indonesia")

        # 3) kabupaten fallback: "kabupaten, Bali, Indonesia"
        if kabupaten:
            query_attempts.append(f"{kabupaten}, Bali, Indonesia")

        # 4) last resort: "Bali, Indonesia" (should not be needed if kabupaten exists)
        query_attempts.append("Bali, Indonesia")

        # Try geocoding in order until success
        found_lat = None
        found_lon = None
        found_label = None
        found_display = None
        used_query = None
        used_source = None

        for q in query_attempts:
            if q in tried_queries and q in geocode_cache:
                lat, lon, display = geocode_cache[q]
            else:
                lat, lon, display = geocode_cached(q)
                tried_queries.add(q)

            if lat is not None and lon is not None:
                found_lat, found_lon, found_display = lat, lon, display
                used_query = q
                # If q came from a candidate phrase (contains candidate before a comma), mark source as article_place
                if "," in q and any(c in q for c in candidates):
                    used_source = "article_place"
                elif kabupaten and kabupaten in q:
                    used_source = "kabupaten_fallback"
                else:
                    used_source = "bali_fallback"
                break

        # If nothing found, skip
        if found_lat is None:
            skipped += 1
            props["refine_status"] = "no_geocode_found"
            props["refined_location"] = None
            props["refine_source"] = None
            # keep original geometry if any
            features[i]["properties"] = props
            csv_rows.append({
                **props,
                "refined_lat": None,
                "refined_lon": None,
                "refined_location_query": None,
                "refine_source": None
            })
            continue

        # Update properties and geometry
        props["refined_location_query"] = used_query
        props["refined_location"] = found_display or used_query
        props["refine_source"] = used_source
        props["latitude_refined"] = found_lat
        props["longitude_refined"] = found_lon

        # Replace geometry (Point)
        features[i]["geometry"] = {
            "type": "Point",
            "coordinates": [found_lon, found_lat]
        }

        # Also update common property names for compatibility
        props["latitude"] = found_lat
        props["longitude"] = found_lon

        features[i]["properties"] = props
        updated += 1

        csv_rows.append({
            **props,
            "refined_lat": found_lat,
            "refined_lon": found_lon,
            "refined_location_query": used_query,
            "refine_source": used_source
        })

        # be polite (RateLimiter also delays, but this separate sleep is optional)
        time.sleep(0.5)

    except Exception as e:
        errors += 1
        # keep existing feature but annotate error
        props = feature.get("properties", {})
        props["refine_error"] = str(e)
        features[i]["properties"] = props
        csv_rows.append({
            **props,
            "refined_lat": None,
            "refined_lon": None,
            "refined_location_query": None,
            "refine_source": "error"
        })
        # continue

# ---- save results ----
data["features"] = features

with open(OUTPUT, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=2)

print(f"\nSaved refined GeoJSON -> {OUTPUT}")
print(f"Updated: {updated}, Skipped (no geocode): {skipped}, Errors: {errors}")

# Optional: save CSV backup
try:
    import pandas as pd
    df_backup = pd.DataFrame(csv_rows)
    df_backup.to_csv(CSV_BACKUP, index=False)
    print(f"Saved CSV backup -> {CSV_BACKUP}")
except Exception:
    pass

# ---- end ----
