import requests
import json

TOKEN = "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"
API_URL = "https://api-free.itick.org/stock/kline"
HEADERS = {
    "accept": "application/json",
    "token": TOKEN
}

params = {
    "region": "NG",
    "code": "MTNN",
    "kType": "8",
    "limit": "5"
}

response = requests.get(API_URL, params=params, headers=HEADERS)
print(f"Status Code: {response.status_code}")
data = response.json()
print(json.dumps(data, indent=2))
