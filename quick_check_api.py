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
    "limit": "1"
}

try:
    response = requests.get(API_URL, params=params, headers=HEADERS)
    data = response.json()
    print(f"Keys: {list(data.keys())}")
    print(f"Data type: {type(data.get('data'))}")
    if isinstance(data.get('data'), list):
         print(f"First kline: {data['data'][0] if data['data'] else 'Empty'}")
except Exception as e:
    print(f"Error: {e}")
