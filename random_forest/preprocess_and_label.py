import os
import pandas as pd
import re

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

if __name__ == "__main__":
    DATA_DIR = '../final_data'
    OUTPUT_DIR = './output_rf'
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("Memuat dan menggabungkan data dari final_data...")
    df = load_and_prepare_data(DATA_DIR)
    print(f"Total data: {len(df)}")
    if len(df) == 0:
        print("Tidak ada data yang bisa diproses.")
        exit()
    print("Melakukan pelabelan sentimen otomatis (rule-based)...")
    df['sentiment_label'] = df['cleaned_content'].apply(label_sentiment_rule_based)
    labelled_path = os.path.join(OUTPUT_DIR, 'labelled_final_data.csv')
    df.to_csv(labelled_path, index=False)
    print(f"Data labelled disimpan di {labelled_path}")
