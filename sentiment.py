import pandas as pd
import glob
import os
import time
import json
import re
from tqdm import tqdm
import requests
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.model_selection import train_test_split
from sklearn.svm import SVC
from sklearn.metrics import classification_report, accuracy_score
import joblib

API_KEY = "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" 

INPUT_FOLDER = './' 
OUTPUT_FILE = 'labeled_sentiment_data.csv'
MODEL_FILE = 'svm_sentiment_model.pkl'

def get_llm_label(text):
    """
    Asks the Teacher (DeepSeek via OpenRouter) to classify the sentiment.
    Returns: 'Positive', 'Negative', or 'Neutral'
    """
    
    prompt_content = f"""
    Analyze the government's response in this disaster news snippet.
    Classify the sentiment as: Positive, Negative, or Neutral.
    
    Rules:
    - Negative: Slow aid, neglect, complaints, lack of preparation.
    - Positive: Fast response, effective aid, appreciation, warnings issued on time.
    - Neutral: Factual reporting without praise or criticism.

    Return ONLY the classification word (Positive, Negative, or Neutral). Do NOT write a sentence.
    
    News Snippet:
    {text[:2000]}
    """

    try:
        response = requests.post(
            url="https://openrouter.ai/api/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {API_KEY}",
                "Content-Type": "application/json",
                "HTTP-Referer": "http://localhost:3000", 
                "X-Title": "ThesisResearch"
            },
            data=json.dumps({
                "model": "deepseek/deepseek-chat",
                "messages": [
                    {"role": "user", "content": prompt_content}
                ],
                "temperature": 0.1,
                "max_tokens": 50 
            })
        )
        
        if response.status_code == 200:
            result = response.json()
            raw_content = result["choices"][0]["message"]["content"].strip()
            
            content_lower = raw_content.lower()
            
            if "negative" in content_lower: return "Negative"
            if "positive" in content_lower: return "Positive"
            if "neutral" in content_lower: return "Neutral"
            
            return "Neutral" 
        else:
            return None

    except Exception as e:
        return None

print("--- PHASE 1: Loading & Merging Data ---")
all_files = glob.glob(os.path.join(INPUT_FOLDER, "filtered_processed_*.csv"))
dfs = []

for filename in all_files:
    try:
        df_temp = pd.read_csv(filename)
        base = os.path.basename(filename).replace('.csv', '')
        parts = base.split('_') 
        if len(parts) >= 5:
            df_temp['Location'] = parts[2]
            df_temp['Disaster'] = parts[3]
            df_temp['Source'] = parts[4]
        dfs.append(df_temp)
    except Exception as e:
        print(f"Skipping bad file: {filename}")

if not dfs:
    print("No data found! Check your folder path.")
    exit()

df_master = pd.concat(dfs, ignore_index=True)
print(f"Loaded {len(df_master)} total articles.")

print("\n--- PHASE 2: Teacher Labeling (DeepSeek) ---")

if os.path.exists(OUTPUT_FILE):
    print("Found existing labeled data. Loading...")
    df_labeled = pd.read_csv(OUTPUT_FILE)
else:
    print("Starting labeling process...")
    df_labeled = df_master.copy()
    
    tqdm.pandas(desc="Labeling Rows")
    df_labeled['sentiment_label'] = df_labeled['cleaned_content'].progress_apply(get_llm_label)
    
    df_labeled.to_csv(OUTPUT_FILE, index=False)
    print(f"Labels generated and saved to {OUTPUT_FILE}")

df_clean = df_labeled.dropna(subset=['sentiment_label', 'cleaned_content'])
print(f"Valid Labeled Data: {len(df_clean)} samples")

print("\n--- PHASE 3: Student Training (SVM) ---")

if len(df_clean) < 10:
    print("Not enough data to train (need at least 10 labeled rows). Exiting.")
    exit()

print("Vectorizing text (TF-IDF)...")
vectorizer = TfidfVectorizer(max_features=5000)
X = vectorizer.fit_transform(df_clean['cleaned_content'])
y = df_clean['sentiment_label']

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

print("Training Support Vector Machine...")
svm_student = SVC(kernel='linear')
svm_student.fit(X_train, y_train)

print("\n--- STUDENT REPORT CARD ---")
predictions = svm_student.predict(X_test)
print("Accuracy:", accuracy_score(y_test, predictions))
print("\nClassification Report:\n")
print(classification_report(y_test, predictions))

joblib.dump(svm_student, MODEL_FILE)
joblib.dump(vectorizer, 'tfidf_vectorizer.pkl')
print("✅ Done! Student model saved.")