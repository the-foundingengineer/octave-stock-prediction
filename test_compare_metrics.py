import requests
import json

BASE_URL = "http://localhost:8000"

def test_compare_metrics():
    # symbols = "DANGCEM,MTNN"
    # metric = "market_cap"
    
    # First, let's get some symbols from the DB
    try:
        response = requests.get(f"{BASE_URL}/stocks?limit=5")
        stocks = response.json()
        if not stocks:
            print("No stocks found in database to test with.")
            return
        
        symbols = ",".join([s["symbol"] for s in stocks[:3]])
        print(f"Testing with symbols: {symbols}")
        
    except Exception as e:
        print(f"Error fetching stocks: {e}")
        return

    metrics = ["market_cap", "revenue", "pe_ratio"]
    
    for metric in metrics:
        print(f"\n--- Testing metric: {metric} ---")
        url = f"{BASE_URL}/stocks/compare-metrics?symbols={symbols}&metric={metric}&limit=5"
        try:
            response = requests.get(url)
            print(f"Status Code: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"Metric in response: {data['metric']}")
                for comp in data["comparisons"]:
                    print(f"Stock: {comp['symbol']}, Data points: {len(comp['data'])}")
                    if comp['data']:
                        print(f"  First point: {comp['data'][0]}")
            else:
                print(f"Error: {response.text}")
        except Exception as e:
            print(f"Request failed: {e}")

if __name__ == "__main__":
    test_compare_metrics()
