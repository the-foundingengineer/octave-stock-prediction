import sys
import os
sys.path.append(os.path.join(os.getcwd(), "app"))
import models
from database import SessionLocal

db = SessionLocal()
try:
    print("Attempting to insert one record manually...")
    record = models.DailyKline(
        symbol="AIRTELAFRI",
        date="2026-01-17",
        timestamp=1768780800000,
        open=2270.0,
        high=2270.0,
        low=2270.0,
        close=2270.0,
        volume=3872,
        turnover=8789440.0
    )
    db.add(record)
    db.commit()
    print("Success! Manual insertion committed.")
    
    # Check if it's there
    count = db.query(models.DailyKline).filter_by(symbol="AIRTELAFRI").count()
    print(f"Count for AIRTELAFRI: {count}")

except Exception as e:
    import traceback
    print(f"Failed: {e}")
    traceback.print_exc()
    db.rollback()
finally:
    db.close()
