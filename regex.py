import os
import re
import pandas as pd
from bs4 import BeautifulSoup

# Define the folder path
folder_path = './raw_old'

disaster = ['banjir', 'gempa', 'tanah longsor', 'erupsi', 'badai', 'longsor']
# Function to clean content
def clean_content(content):
    # Case folding
    content = content.lower()
    # Remove special characters and symbols
    content = re.sub(r'[^a-zA-Z0-9\s]', '', content)
    # Remove HTML tags
    content = BeautifulSoup(content, 'html.parser').get_text()
    # Remove extra whitespace, including newlines, from the content
    content = re.sub(r'\s+', ' ', content).strip()
    return content

# Function to extract metadata from filename
def extract_metadata(filename):
    match = re.match(r'(\w+)_(\w+)_(\w+)\.csv', filename, re.IGNORECASE)
    if match:
        return {
            'name': None,  # no name prefix
            'number': None,
            'location': match.group(1).lower(),
            'disaster': match.group(2).lower(),
            'news_portal': match.group(3).lower()
        }
    return None

# Function to verify content with NER
def verify_content(content, metadata):
    # Check if the disaster and location are mentioned in the content
    disaster_mentioned = any(d in content for d in disaster if d == metadata['disaster'])
    location_mentioned = metadata['location'] in content
    return disaster_mentioned and location_mentioned

total_valid_rows = 0  # Initialize a counter for total valid rows

# Process each file in the folder
for filename in os.listdir(folder_path):
    if filename.endswith('.csv'):
        metadata = extract_metadata(filename)
        if metadata:
            file_path = os.path.join(folder_path, filename)
            # Skip processing if the CSV file is empty or has no columns
            try:
                df = pd.read_csv(file_path)
            except pd.errors.EmptyDataError:
                print(f'Skipping empty or invalid file: {filename}')
                continue

            if 'content' in df.columns:
                # Clean the content
                df['cleaned_content'] = df['content'].apply(clean_content)
                # Verify content
                df['is_valid'] = df['cleaned_content'].apply(lambda x: verify_content(x, metadata))
                # Save only valid rows to the processed file
                valid_rows = df[df['is_valid']]
                clean_folder_path = './clean'
                os.makedirs(clean_folder_path, exist_ok=True)
                processed_file_path = os.path.join(clean_folder_path, f'processed_{filename}')
                valid_rows.drop(columns=['content']).to_csv(processed_file_path, index=False)
                before_count = len(df)
                valid_count = len(valid_rows)
                total_valid_rows += valid_count  # Update the total valid rows counter
                print(f'Jumlah baris sebelum: {before_count}, setelah (valid): {valid_count}')

# Print the total number of valid rows across all files
print(f'Total valid rows across all files: {total_valid_rows}')