import requests
import json

TOKEN = "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"

def check_split_api():
    # Trying api-free first as user is on free plan
    url = "https://api-free.itick.org/stock/split"
    params = {
        "region": "NG",
        # "type": "stock" # user didn't have this in the link but might be needed? Link was https://api.itick.org/stock/split?region=NG
    }
    headers = {
        "accept": "application/json",
        "token": TOKEN
    }

    print(f"Fetching from {url}...")
    try:
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        print(json.dumps(data, indent=2))
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_split_api()
