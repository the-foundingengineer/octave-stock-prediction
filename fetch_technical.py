import requests
import sys

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Accept-Language": "en-US,en;q=0.9",
}

try:
    print("Fetching...")
    url = "https://ng.investing.com/equities/mtn-nigeria-com-technical"
    response = requests.get(url, headers=HEADERS, timeout=10)
    print("Status:", response.status_code)
    print("Length:", len(response.text))
    with open("tech.html", "w", encoding="utf-8") as f:
        f.write(response.text)
    print("Saved to tech.html")
except Exception as e:
    print("Error:", e)
