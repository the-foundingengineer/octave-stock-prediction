import sys
import os
sys.path.append(os.path.join(os.getcwd(), "app"))
import models
from database import SessionLocal

db = SessionLocal()
try:
    stocks = db.query(models.Stock).limit(20).all()
    for s in stocks:
        print(f"|{s.symbol}|")
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
