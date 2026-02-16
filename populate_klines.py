import time
import datetime
import sys
import os
import requests
from sqlalchemy.orm import Session

sys.path.append(os.path.join(os.getcwd(), "app"))

import models
from database import SessionLocal, engine
from stock_codes import STOCK_CODES

# Create tables if they don't exist (will create daily_klines)
models.Base.metadata.create_all(bind=engine)

TOKEN = "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c"
API_URL = "https://api-free.itick.org/stock/kline"
HEADERS = {
    "accept": "application/json",
    "token": TOKEN
}

LIMIT = 1095      # ~3 years of trading days
K_TYPE = 8       # Daily kline

def fetch_klines(code: str) -> dict:
    """Fetch daily klines for a single stock code."""
    params = {
        "region": "NG",
        "code": code,
        "kType": str(K_TYPE),
        "limit": str(LIMIT)
    }
    response = requests.get(API_URL, params=params, headers=HEADERS)
    response.raise_for_status()
    return response.json()

def timestamp_to_date(ts_ms: int) -> str:
    """Convert epoch milliseconds to YYYY-MM-DD."""
    return datetime.datetime.fromtimestamp(ts_ms / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d")

def populate_klines():
    db: Session = SessionLocal()
    
    try:
        # Clear existing daily klines
        print("Clearing existing daily_klines table...")
        db.query(models.DailyKline).delete()
        db.commit()
        
        total_records = 0
        total_stocks = len(STOCK_CODES)
        
        print(f"Fetching klines for {total_stocks} stocks individually (rate limited to 5 calls/min)...")
        
        for idx, symbol in enumerate(STOCK_CODES):
            print(f"\n[{idx+1}/{total_stocks}] {symbol}...")
            
            try:
                data = fetch_klines(symbol)
                
                if data.get("code") != 0:
                    print(f"  API Error: {data.get('msg')}")
                    # If it's a rate limit error, we might want to sleep longer
                    if "rate" in str(data.get("msg")).lower():
                        print("  Rate limit detected, sleeping for 60s...")
                        time.sleep(60)
                    continue
                
                klines = data.get("data", [])
                if not isinstance(klines, list):
                    print(f"  Unexpected data format for {symbol}")
                    continue

                count = 0
                for kline in klines:
                    ts = kline.get("t")
                    date_str = timestamp_to_date(ts) if ts else None
                    
                    if date_str and date_str < "2023-01-01":
                        continue
                    
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
                
                total_records += count
                print(f"  {symbol}: {count} records added")
                db.commit()
                
            except Exception as e:
                print(f"  {symbol} failed: {e}")
                db.rollback()
            
            # Rate limit: 5 calls per minute = 12 seconds delay
            if idx < total_stocks - 1:
                time.sleep(12)
        
        print(f"\nDone! Total records inserted: {total_records}")
        
    except Exception as e:
        import traceback
        print(f"Fatal error: {e}")
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    populate_klines()
