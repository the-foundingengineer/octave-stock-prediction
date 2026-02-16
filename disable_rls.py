import sys
import os
from sqlalchemy import text
from app.database import engine

def disable_rls():
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        try:
            print("Disabling RLS on daily_klines...")
            conn.execute(text("ALTER TABLE daily_klines DISABLE ROW LEVEL SECURITY"))
            print("Done.")
            
            # Verify RLS status
            result = conn.execute(text("SELECT relrowsecurity FROM pg_class WHERE relname='daily_klines'")).scalar()
            print(f"RLS Enabled (should be False): {result}")
            
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    disable_rls()
