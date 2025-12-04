import json
import requests

API_KEY = "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" 

def test_api():
    url = "https://openrouter.ai/api/v1/chat/completions"

    prompt = "Classify this text as Positive, Negative, or Neutral: The government responded quickly."

    payload = {
        "model": "deepseek/deepseek-chat",
        "messages": [{"role": "user", "content": prompt}],
        "temperature": 0.1,
        "max_tokens": 20
    }

    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "http://localhost:3000",
        "X-Title": "ThesisResearch"
    }

    print("➡️ Sending request...")
    response = requests.post(url, headers=headers, data=json.dumps(payload))

    print("➡️ Status Code:", response.status_code)

    if response.status_code != 200:
        print("\n❌ API returned an error:\n", response.text)
        return

    try:
        data = response.json()
        print("\n✅ API Response JSON:\n", json.dumps(data, indent=2))

        result = data["choices"][0]["message"]["content"].strip()
        print("\n🎉 Extracted Model Output:", result)

    except Exception as e:
        print("\n❌ Failed to parse JSON:", e)
        print(response.text)

if __name__ == "__main__":
    test_api()
