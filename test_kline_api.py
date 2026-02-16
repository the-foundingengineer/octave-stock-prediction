import requests
import json

URL = "https://api-free.itick.org/stock/klines"
HEADERS = {
    "accept": "application/json",
    "token": "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"
}

params = {
    "region": "NG",
    "codes": "MTNN,DANGCEM",
    "kType": "8",
    "limit": "3"
}

response = requests.get(URL, params=params, headers=HEADERS)
response.raise_for_status()
data = response.json()

with open("kline_sample.json", "w") as f:
    json.dump(data, f, indent=2)

print("Saved to kline_sample.json")
