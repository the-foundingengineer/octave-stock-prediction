"""Check DB state - suppresses SQLAlchemy logging."""
import logging
logging.disable(logging.CRITICAL)

from app.database import SessionLocal
from sqlalchemy import text
import sys

sys.stdout.reconfigure(encoding='utf-8', errors='replace')

db = SessionLocal()
try:
    r = db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = 'daily_klines' ORDER BY ordinal_position"
    ))
    cols = [row[0] for row in r]
    print(f"daily_klines: {len(cols)} columns")
    for c in cols:
        print(f"  {c}")
    print()

    for tbl in ["stocks", "daily_klines", "revenue_history", "dividends", "stock_ratios", "income_statements"]:
        try:
            cnt = db.execute(text(f"SELECT COUNT(*) FROM {tbl}")).scalar()
            print(f"{tbl}: {cnt} rows")
        except Exception:
            print(f"{tbl}: NOT FOUND")
            db.rollback()
except Exception as e:
    print(f"Error: {e}")
finally:
    db.close()
