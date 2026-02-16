import time
import os
from sqlalchemy.orm import Session
from app.database import SessionLocal
from app import models
from services import update_stock_info
from dotenv import load_dotenv

load_dotenv()

TOKEN = os.getenv("ITICK_TOKEN", "94e8022ec421488092b29bfe345140fa03db4d3cd9224f628b34dd1e9f71ba4c")

def populate_sectors():
    db: Session = SessionLocal()
    try:
        stocks = db.query(models.Stock).all()
        total = len(stocks)
        print(f"Found {total} stocks in database.")

        for i, stock in enumerate(stocks):
            print(f"[{i+1}/{total}] Updating sector for {stock.symbol}...")
            try:
                update_stock_info(db, stock.symbol, TOKEN)
                # Rate limiting to avoid throttling
                time.sleep(0.5)
            except Exception as e:
                print(f"Error updating {stock.symbol}: {e}")
                continue
        
        print("Sectors update completed.")
    finally:
        db.close()

if __name__ == "__main__":
    populate_sectors()
