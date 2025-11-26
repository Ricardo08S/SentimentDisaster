import os
import pandas as pd
import re
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

DATA_DIR = '../final_data'
OUTPUT_DIR = './output_rf'
os.makedirs(OUTPUT_DIR, exist_ok=True)

def list_data_files(data_dir):
    return [f for f in os.listdir(data_dir) if f.startswith('filtered_processed_') and f.endswith('.csv')]

def preprocess_text(text):
    if pd.isnull(text):
        return ''
    text = text.lower()
    text = re.sub(r'[^a-zA-Z\s]', '', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def load_and_prepare_data(data_dir):
    files = list_data_files(data_dir)
    dfs = []
    for file in files:
        path = os.path.join(data_dir, file)
        try:
            df = pd.read_csv(path)
            if 'cleaned_content' in df.columns and 'is_valid' in df.columns:
                df = df[df['is_valid'] == True]
                df = df.dropna(subset=['cleaned_content'])
                df['cleaned_content'] = df['cleaned_content'].apply(preprocess_text)
                dfs.append(df)
        except Exception as e:
            print(f"Failed to load {file}: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

def label_sentiment_rule_based(text):
    if 'ancaman' in text or 'bahaya' in text or 'waspada' in text or 'bencana' in text:
        return 'Negative'
    elif 'siaga' in text or 'antisipasi' in text or 'selamat' in text:
        return 'Neutral'
    elif 'aman' in text or 'berhasil' in text or 'selamat' in text:
        return 'Positive'
    else:
        return 'Neutral'

def run_random_forest(df, text_column='cleaned_content', label_column='sentiment_label', output_prefix='rf'):
    if text_column not in df.columns or label_column not in df.columns:
        print(f"Kolom {text_column} atau {label_column} tidak ditemukan!")
        return
    X = df[text_column]
    y = df[label_column]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
    vectorizer = TfidfVectorizer(max_features=1000)
    X_train_vec = vectorizer.fit_transform(X_train)
    X_test_vec = vectorizer.transform(X_test)
    clf = RandomForestClassifier(n_estimators=100, random_state=42)
    clf.fit(X_train_vec, y_train)
    y_pred = clf.predict(X_test_vec)
    report = classification_report(y_test, y_pred)
    cm = confusion_matrix(y_test, y_pred)
    print("\nClassification Report:")
    print(report)
    print("Confusion Matrix:")
    print(cm)
    with open(os.path.join(OUTPUT_DIR, f'{output_prefix}_classification_report.txt'), 'w', encoding='utf-8') as f:
        f.write('Classification Report\n')
        f.write(report)
        f.write('\nConfusion Matrix\n')
        f.write(str(cm))
    plt.figure(figsize=(6, 4))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=clf.classes_, yticklabels=clf.classes_)
    plt.xlabel('Predicted')
    plt.ylabel('True')
    plt.title('Confusion Matrix')
    plt.tight_layout()
    plt.savefig(os.path.join(OUTPUT_DIR, f'{output_prefix}_confusion_matrix.png'))
    plt.close()

if __name__ == "__main__":
    print("Memuat dan menggabungkan data dari final_data...")
    df = load_and_prepare_data(DATA_DIR)
    print(f"Total data: {len(df)}")
    if len(df) == 0:
        print("Tidak ada data yang bisa diproses.")
        exit()
    print("Melakukan pelabelan sentimen otomatis (rule-based)...")
    df['sentiment_label'] = df['cleaned_content'].apply(label_sentiment_rule_based)
    df.to_csv(os.path.join(OUTPUT_DIR, 'labelled_final_data.csv'), index=False)
    print("Training dan evaluasi Random Forest...")
    run_random_forest(df, output_prefix='rf')
    print(f"Hasil evaluasi dan data labelled disimpan di folder {OUTPUT_DIR}")
