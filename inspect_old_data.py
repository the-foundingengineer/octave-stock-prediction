import sys
import os
from datetime import datetime
sys.path.append(os.path.join(os.getcwd(), "app"))
from database import engine
from sqlalchemy import text

def inspect_data():
    with engine.connect() as conn:
        print("Checking old columns in 'stocks' table...")
        query = text("""
            SELECT symbol, outstanding_shares, market_cap, pe_ratio, 
                   fifty_two_week_high, fifty_two_week_low, adjustment_factor 
            FROM stocks 
            LIMIT 10
        """)
        res = conn.execute(query)
        rows = [dict(r._mapping) for r in res]
        
        for row in rows:
            print(f"Stock: {row['symbol']}")
            print(f"  OS: {row['outstanding_shares']} | MC: {row['market_cap']} | PE: {row['pe_ratio']}")
            print(f"  52H: {row['fifty_two_week_high']} | 52L: {row['fifty_two_week_low']} | AF: {row['adjustment_factor']}")
            print("-" * 20)

if __name__ == "__main__":
    inspect_data()
