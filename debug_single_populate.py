import datetime
import sys
import os
import requests
from sqlalchemy.orm import Session

sys.path.append(os.path.join(os.getcwd(), "app"))

import models
from database import SessionLocal, engine

TOKEN = "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"
API_URL = "https://api-free.itick.org/stock/klines"
HEADERS = {
    "accept": "application/json",
    "token": TOKEN
}

def timestamp_to_date(ts_ms: int) -> str:
    return datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")

def debug_populate():
    db: Session = SessionLocal()
    try:
        symbol = "AIRTELAFRI"
        params = {
            "region": "NG",
            "codes": symbol,
            "kType": "8",
            "limit": "10"
        }
        print(f"Fetching for {symbol}...")
        response = requests.get(API_URL, params=params, headers=HEADERS)
        data = response.json()
        
        kline_data = data.get("data", {})
        klines = kline_data.get(symbol, [])
        print(f"Found {len(klines)} klines for {symbol}")
        
        count = 0
        for kline in klines:
            ts = kline.get("t")
            date_str = timestamp_to_date(ts) if ts else None
            print(f"  Processing {date_str} (ts: {ts})")
            
            record = models.DailyKline(
                symbol=symbol,
                date=date_str,
                timestamp=ts,
                open=kline.get("o"),
                high=kline.get("h"),
                low=kline.get("l"),
                close=kline.get("c"),
                volume=int(kline.get("v", 0)) if kline.get("v") is not None else None,
                turnover=kline.get("tu")
            )
            db.add(record)
            count += 1
        
        print(f"Committing {count} records...")
        db.commit()
        print("Commit successful")
        
    except Exception as e:
        print(f"Error: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    debug_populate()
