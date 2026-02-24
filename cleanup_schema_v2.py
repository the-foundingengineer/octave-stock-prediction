
import sys
import os
from sqlalchemy import text

# Add current directory to path so 'app' can be found
sys.path.append(os.getcwd())

try:
    from app.database import engine
    print("Database engine imported successfully.")
except ImportError as e:
    print(f"Failed to import database engine: {e}")
    sys.exit(1)

def cleanup():
    print("Starting cleanup function...")
    conn = engine.connect()
    try:
        print("Cleaning up 'stocks' table (explicit commit mode)...")
        
        # 1. Add 'isin'
        print("Ensuring 'isin' column exists...")
        try:
            conn.execute(text("ALTER TABLE stocks ADD COLUMN IF NOT EXISTS isin VARCHAR(20)"))
            conn.commit()
            print("  [SUCCESS] isin column processed.")
        except Exception as e:
            print(f"  [ERROR] adding 'isin': {e}")
            conn.rollback()

        # 2. Drop legacy columns
        legacy_stocks_cols = [
            "market_cap", "pe_ratio", "fifty_two_week_high", "fifty_two_week_low",
            "forward_pe", "ps_ratio", "pb_ratio", "dividend_per_share", 
            "dividend_yield", "ex_dividend_date", "adjustment_factor"
        ]
        
        for col in legacy_stocks_cols:
            print(f"Dropping legacy column '{col}'...")
            try:
                conn.execute(text(f"ALTER TABLE stocks DROP COLUMN IF EXISTS {col}"))
                conn.commit()
                print(f"  [SUCCESS] {col} dropped.")
            except Exception as e:
                print(f"  [ERROR] dropping '{col}': {e}")
                conn.rollback()

        # 3. Fix 'last_updated'
        print("Fixing 'last_updated' column...")
        try:
            conn.execute(text("ALTER TABLE stocks DROP COLUMN IF EXISTS last_updated"))
            conn.execute(text("ALTER TABLE stocks ADD COLUMN last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP"))
            conn.commit()
            print("  [SUCCESS] last_updated reset.")
        except Exception as e:
            print(f"  [ERROR] fixing 'last_updated': {e}")
            conn.rollback()

        print("\nCleanup sequence finished.")
    except Exception as e:
        print(f"Unexpected error during cleanup: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    cleanup()
