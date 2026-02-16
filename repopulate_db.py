import csv
import sys
import os
from sqlalchemy.orm import Session

# Ensure the 'app' directory is in the path
sys.path.append(os.path.join(os.getcwd(), "app"))

import models
from database import SessionLocal, engine

def repopulate_data(file_path: str):
    """
    Clears stock tables and imports data from the formatted CSV file.
    """
    db: Session = SessionLocal()
    try:
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return

        # 1. Clear existing data
        print("Clearing existing stock records and stocks...")
        db.query(models.StockRecord).delete()
        db.query(models.Stock).delete()
        db.commit()

        # 2. Import from formatted CSV
        print(f"Importing data from {file_path}...")
        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            count = 0
            seen_stocks = {} # ticker -> name
            
            for row in reader:
                # date,open,high,low,close,volume,stock_name,ticker
                ticker = row.get("ticker")
                name = row.get("stock_name")
                
                if ticker and ticker != "UNKNOWN":
                    if ticker not in seen_stocks:
                        seen_stocks[ticker] = name
                
                # Create record
                # Note: models.StockRecord fields match these names
                stock_record = models.StockRecord(
                    date=row.get("date"),
                    open=row.get("open"),
                    high=row.get("high"),
                    low=row.get("low"),
                    close=row.get("close"),
                    volume=row.get("volume"),
                    stock_name=ticker if ticker and ticker != "UNKNOWN" else name
                )
                
                db.add(stock_record)
                count += 1
                
                if count % 5000 == 0:
                    db.commit()
                    print(f"Imported {count} records...")

            # 3. Populate stocks table with unique tickers
            print(f"Populating stocks table with {len(seen_stocks)} unique stocks...")
            for ticker, name in seen_stocks.items():
                stock_entry = models.Stock(symbol=ticker, name=name)
                db.add(stock_entry)
            
            db.commit()
            print(f"Successfully imported {count} records and {len(seen_stocks)} stocks.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    csv_file_path = "formatted_stocks.csv"
    repopulate_data(csv_file_path)
