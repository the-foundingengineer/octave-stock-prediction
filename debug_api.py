import requests
import json

TOKEN = "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"
API_URL = "https://api-free.itick.org/stock/klines"
HEADERS = {
    "accept": "application/json",
    "token": TOKEN
}

batch = ["AIRTELAFRI", "BUAFOODS", "MTNN"]
params = {
    "region": "NG",
    "codes": ",".join(batch),
    "kType": "8",
    "limit": "800"
}

response = requests.get(API_URL, params=params, headers=HEADERS)
print(f"Status Code: {response.status_code}")
data = response.json()
print(f"Response Code: {data.get('code')}")
print(f"Response Msg: {data.get('msg')}")

klines_data = data.get("data", {})
for symbol, klines in klines_data.items():
    print(f"Symbol: {symbol}, Klines Count: {len(klines)}")
    if klines:
        print(f"Sample Kline: {klines[0]}")

with open("api_debug_output.json", "w") as f:
    json.dump(data, f, indent=2)
