import requests
import json

URL = "https://api-free.itick.org/symbol/list?type=stock&region=NG"
HEADERS = {
    "accept": "application/json",
    "token": "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"
}

def fetch_stock_codes():
    response = requests.get(URL, headers=HEADERS)
    response.raise_for_status()
    data = response.json()

    # The API returns a list of products with keys: c, n, t, e
    products = data.get("data", data)  # Try "data" key first, fallback to root

    if isinstance(products, dict):
        # Maybe the list is nested differently
        print("Response structure:", list(products.keys()))
        # Try common patterns
        for key in products:
            if isinstance(products[key], list):
                products = products[key]
                break

    codes = [item["c"] for item in products if "c" in item]

    print(f"Total products found: {len(codes)}")
    print(f"\nAll codes:\n{json.dumps(codes, indent=2)}")

    # Save to a Python file for easy reuse
    with open("stock_codes.py", "w") as f:
        f.write(f"# Fetched from iTick API on 2026-02-13\n")
        f.write(f"# Total: {len(codes)} stocks\n\n")
        f.write(f"STOCK_CODES = {json.dumps(codes, indent=4)}\n")

    print(f"\nSaved to stock_codes.py")

if __name__ == "__main__":
    fetch_stock_codes()
