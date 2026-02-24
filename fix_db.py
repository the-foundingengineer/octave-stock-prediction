from app.database import engine
from sqlalchemy import text

def fix():
    with engine.connect() as conn:
        print("Fixing revenue_history.fiscal_year_end with explicit cast...")
        conn.execute(text("ALTER TABLE revenue_history ALTER COLUMN fiscal_year_end TYPE VARCHAR USING fiscal_year_end::VARCHAR"))
        conn.commit()
        
        print("Fixing dividends columns with explicit cast...")
        conn.execute(text("ALTER TABLE dividends ALTER COLUMN ex_dividend_date TYPE VARCHAR USING ex_dividend_date::VARCHAR"))
        conn.execute(text("ALTER TABLE dividends ALTER COLUMN record_date TYPE VARCHAR USING record_date::VARCHAR"))
        conn.execute(text("ALTER TABLE dividends ALTER COLUMN pay_date TYPE VARCHAR USING pay_date::VARCHAR"))
        conn.commit()
        
    print("Fix finished successfully.")

if __name__ == "__main__":
    fix()
