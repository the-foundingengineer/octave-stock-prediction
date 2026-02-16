import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

def check_raw():
    print(f"Connecting to: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'UNKNOWN'}")
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Check stock_records
        cur.execute("SELECT count(*) FROM stock_records")
        count_records = cur.fetchone()[0]
        print(f"stock_records count: {count_records}")
        
        # Check daily_klines
        cur.execute("SELECT count(*) FROM daily_klines")
        count_klines = cur.fetchone()[0]
        print(f"daily_klines count: {count_klines}")
        
        # Check if daily_kline (singular) exists
        try:
            cur.execute("SELECT count(*) FROM daily_kline")
            count_singular = cur.fetchone()[0]
            print(f"daily_kline (singular) count: {count_singular}")
        except Exception as e:
            print("daily_kline (singular) table does not exist")
            conn.rollback() # Reset transaction after error

        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"Connection failed: {e}")

if __name__ == "__main__":
    check_raw()
