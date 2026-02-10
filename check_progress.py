import sys
import os
sys.path.append(os.getcwd())
from app import models
from app.database import SessionLocal

def check_progress():
    db = SessionLocal()
    try:
        count = db.query(models.Stock).count()
        print(f"Stocks in Stock table: {count}")
    finally:
        db.close()

if __name__ == "__main__":
    check_progress()
