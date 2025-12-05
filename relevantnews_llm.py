import requests
import json
import os
import pandas as pd
import re

# Define parameters for the API request
temperature = 0.1
max_tokens = 10

clean_folder = "./clean"
valid_news_folder = "./valid_news"
os.makedirs(valid_news_folder, exist_ok=True)

def is_news(cleaned_content):
    """Check if the cleaned_content is a news article using LLM."""
    response = requests.post(
        url="https://openrouter.ai/api/v1/chat/completions",
        headers={
            "Authorization": "Bearer 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",
            "Content-Type": "application/json",
        },
        data=json.dumps({
            "model": "deepseek/deepseek-chat",
            "messages": [
                {
                    "role": "user",
                    "content": f"{cleaned_content}'"
                }
            ],
            "temperature": 0.1,
            "max_tokens": 100
        }))

    llm_response = response.json().get('choices', [{}])[0].get('message', {}).get('content', 'No response').strip().lower()
    print(f"LLM Response: {llm_response}")  # Log LLM response in lowercase

    if response.status_code == 200:
        answer = response.json().get("choices", [{}])[0].get("message", {}).get("content", "No").strip().lower()
        return answer == "iya"
    else:
        print(f"Error: {response.status_code}, {response.text}")
        return False

# Extract disaster and location from the file name
def extract_disaster_and_location(file_name):
    match = re.search(r"^(\w+)_(\w+)_", file_name)
    if match:
        location, disaster = match.groups()
        return location, disaster
    return None, None

# Process each CSV file in the clean folder
for file_name in os.listdir(clean_folder):
    if file_name.endswith(".csv"):
        try:
            file_path = os.path.join(clean_folder, file_name)
            print(f"Processing file: {file_name}")
            df = pd.read_csv(file_path)

            if "cleaned_content" in df.columns:
                valid_rows = []

                location, disaster = extract_disaster_and_location(file_name)
                if location and disaster:
                    print(f"Extracted location: {location}, disaster: {disaster}")

                print(f"Total rows before filtering: {len(df)}")  # Log total rows before filtering

                for _, row in df.iterrows():
                    cleaned_content = row["cleaned_content"]
                    cleaned_content = cleaned_content.strip() if isinstance(cleaned_content, str) else ""

                    if len(cleaned_content.split()) < 5:  # Skip if content has less than 5 words
                        print(f"Skipping invalid or too short content: {cleaned_content[:50]}...")
                        continue

                    prompt = f"jawab dengan 'iya' atau 'tidak' saja Apakah bencana alam {disaster} terjadi di daerah {location}? {cleaned_content}"
                    print(f"Checking cleaned_content: {cleaned_content[:50]}...")
                    # print(prompt)
                    if is_news(prompt):
                        valid_rows.append(row)

                if valid_rows:
                    valid_df = pd.DataFrame(valid_rows)
                    print(f"Total rows after filtering: {len(valid_df)}")  # Log total rows after filtering
                    valid_file_path = os.path.join(valid_news_folder, file_name.replace(".csv", "_valid.csv"))
                    valid_df.to_csv(valid_file_path, index=False)
                    print(f"Valid news saved to: {valid_file_path}")
                else:
                    print(f"No valid news found in file: {file_name}")
            else:
                print(f"Column 'cleaned_content' not found in file: {file_name}")
        except Exception as e:
            print(f"Error processing file {file_name}: {e}")

print("Processing complete. Valid news files are saved in the 'valid_news' folder.")

# Calculate and print the total rows processed across all files
total_rows_processed = sum(len(pd.read_csv(os.path.join(clean_folder, file))) for file in os.listdir(clean_folder) if file.endswith('.csv'))
print(f"Total rows processed across all files: {total_rows_processed}")