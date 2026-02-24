from app.database import engine
from sqlalchemy import text

def check():
    with engine.connect() as conn:
        print("--- Table: revenue_history ---")
        res = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'revenue_history'")).fetchall()
        for r in res:
            print(r)
            
        print("\n--- Table: daily_klines ---")
        res = conn.execute(text("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'daily_klines'")).fetchall()
        for r in res:
            if r[0] in ['revenue_ttm', 'revenue_growth', 'ps_ratio', 'payout_ratio', 'fiscal_year_end']:
                print(r)

if __name__ == "__main__":
    check()
