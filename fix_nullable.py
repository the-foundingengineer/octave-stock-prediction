from sqlalchemy import text
from app.database import engine

def fix_nullable():
    with engine.connect() as conn:
        print("Dropping NOT NULL constraints for financial data...")
        try:
            conn.execute(text("ALTER TABLE dividends ALTER COLUMN ex_dividend_date DROP NOT NULL"))
            conn.execute(text("ALTER TABLE dividends ALTER COLUMN amount DROP NOT NULL"))
            conn.execute(text("ALTER TABLE revenue_history ALTER COLUMN fiscal_year_end DROP NOT NULL"))
            conn.execute(text("ALTER TABLE revenue_history ALTER COLUMN revenue DROP NOT NULL"))
            conn.commit()
            print("Successfully updated constraints.")
        except Exception as e:
            print(f"Error updating constraints: {e}")
            conn.rollback()

if __name__ == "__main__":
    fix_nullable()
