import requests
import traceback
from app.database import SessionLocal
from app import models

TOKEN = "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"

def populate_splits():
    url = "https://api-free.itick.org/stock/split"
    params = {"region": "NG"}
    headers = {"accept": "application/json", "token": TOKEN}
    
    db = SessionLocal()
    try:
        print("Fetching stock splits...")
        response = requests.get(url, params=params, headers=headers)
        response.raise_for_status()
        data = response.json()
        
        if data.get("code") != 0:
            print(f"API Error: {data.get('msg')}")
            return

        splits = data.get("data", {}).get("content", [])
        print(f"Found {len(splits)} split records.")
        
        count = 0
        for item in splits:
            symbol = item.get("c")
            factor = item.get("v")
            
            # Find stock
            stock = db.query(models.Stock).filter(models.Stock.symbol == symbol).first()
            if stock:
                stock.adjustment_factor = factor
                count += 1
                print(f"Updated {symbol} with factor {factor}")
        
        db.commit()
        print(f"Successfully updated/matched {count} stocks.")
        
    except Exception as e:
        print(f"Error: {e}")
        traceback.print_exc()
    finally:
        db.close()

if __name__ == "__main__":
    populate_splits()
