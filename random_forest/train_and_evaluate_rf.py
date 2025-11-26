import pandas as pd
import os
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report, confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns

LABELLED_PATH = './output_rf/labelled_final_data.csv'
OUTPUT_DIR = './output_rf'
os.makedirs(OUTPUT_DIR, exist_ok=True)

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
    print(f"Membaca data dari {LABELLED_PATH} ...")
    df = pd.read_csv(LABELLED_PATH)
    print(f"Total data: {len(df)}")
    if len(df) == 0:
        print("Tidak ada data yang bisa diproses.")
        exit()
    print("Training dan evaluasi Random Forest...")
    run_random_forest(df, output_prefix='rf')
    print(f"Hasil evaluasi disimpan di folder {OUTPUT_DIR}")
