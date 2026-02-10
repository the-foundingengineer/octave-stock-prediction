import csv
import sys
import os

# Create tables if they don't exist
# Ensure the 'app' directory is in the path so we can import modules as if running from there
sys.path.append(os.path.join(os.getcwd(), "app"))

import models
from database import SessionLocal, engine

# Create tables if they don't exist
models.Base.metadata.create_all(bind=engine)

def import_data(file_path: str):
    """
    Imports stock data from a CSV file into the database.
    
    Args:
        file_path (str): The path to the CSV file.
    """
    db: Session = SessionLocal()
    try:
        if not os.path.exists(file_path):
            print(f"Error: File not found at {file_path}")
            return

        with open(file_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            count = 0
            # Track unique symbols seen in this batch
            seen_symbols = set()
            existing_stocks = {s[0] for s in db.query(models.Stock.symbol).all()}
            
            for row in reader:
                # Map CSV columns to database model fields
                # CSV Headers: Date,Price,Open,High,Low,Vol.,Change %,symbol,Name,...
                
                stock_record = models.StockRecord(
                    date=row.get("Date"),
                    open=row.get("Open"),
                    high=row.get("High"),
                    low=row.get("Low"),
                    close=row.get("Price"), # Assuming 'Price' is the close price
                    volume=row.get("Vol."),
                    stock_name=row.get("symbol").replace("Stock\\", "") # Using 'symbol' as stock_name and removing 'Stock\' prefix
                )
                
                clean_name = stock_record.stock_name
                if clean_name not in existing_stocks and clean_name not in seen_symbols:
                    stock_entry = models.Stock(symbol=clean_name)
                    db.add(stock_entry)
                    seen_symbols.add(clean_name)

                
                db.add(stock_record)
                count += 1
                
                # Commit in batches (optional, but good for large files)
                if count % 1000 == 0:
                    db.commit()
                    print(f"Imported {count} records...")
            
            db.commit()
            print(f"Successfully imported {count} records.")
            
    except Exception as e:
        print(f"An error occurred: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    csv_file_path = "all_stocks.csv"
    print(f"Starting import from {csv_file_path}...")
    import_data(csv_file_path)
