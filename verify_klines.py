import sys
import os
from sqlalchemy.orm import Session

# Ensure the 'app' directory is in the path
sys.path.append(os.path.join(os.getcwd(), "app"))

import models
from database import SessionLocal

def verify_klines():
    db: Session = SessionLocal()
    try:
        count = db.query(models.DailyKline).count()
        print(f"Total DailyKline Records: {count}")
        
        print("\nFirst 5 DailyKline Records:")
        klines = db.query(models.DailyKline).limit(5).all()
        for k in klines:
            print(f" - {k.date} | {k.symbol} | Close: {k.close} | Vol: {k.volume}")

        print("\nLast 5 DailyKline Records:")
        klines = db.query(models.DailyKline).order_by(models.DailyKline.id.desc()).limit(5).all()
        for k in klines:
            print(f" - {k.date} | {k.symbol} | Close: {k.close} | Vol: {k.volume}")

    finally:
        db.close()

if __name__ == "__main__":
    verify_klines()
