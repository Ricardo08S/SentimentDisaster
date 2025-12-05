import json
import joblib
import pandas as pd
from tqdm import tqdm

# -------------------------------
# Load models
# -------------------------------
print("Loading TF-IDF vectorizer & SVM model...")
vectorizer = joblib.load("./sentiment_models/tfidf_vectorizer.pkl")
svm_model = joblib.load("./sentiment_models/svm_sentiment_model_unified.pkl")

# -------------------------------
# Load GeoJSON
# -------------------------------
INPUT_GEOJSON = "./geojson/unified_disaster_map.geojson"
OUTPUT_GEOJSON = "./geojson/unified_map_with_sentiment.geojson"

print(f"\nLoading {INPUT_GEOJSON} ...")
with open(INPUT_GEOJSON, "r", encoding="utf-8") as f:
    geo_data = json.load(f)

features = geo_data.get("features", [])
print(f"Total features found: {len(features)}")

# -------------------------------
# Predict sentiment for each article
# -------------------------------
def predict_sentiment(text):
    if not text or str(text).strip() == "":
        return "Neutral"
    X = vectorizer.transform([text])
    return svm_model.predict(X)[0]

print("\nPredicting sentiment for each feature...")
for feature in tqdm(features):
    props = feature.get("properties", {})
    text = props.get("cleaned_content", "")
    sentiment = predict_sentiment(text)
    props["sentiment_label"] = sentiment
    feature["properties"] = props

# -------------------------------
# Save updated GeoJSON
# -------------------------------
print(f"\nSaving updated GeoJSON → {OUTPUT_GEOJSON}")
with open(OUTPUT_GEOJSON, "w", encoding="utf-8") as f:
    json.dump(geo_data, f, ensure_ascii=False, indent=2)

print("\n✅ DONE! Sentiment added for all articles.")
