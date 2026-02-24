from sqlalchemy import text
from app.database import engine
from app import models

def migrate():
    print("Running force migration for new tables...")
    
    with engine.connect() as conn:
        # Drop new tables to recreate with String date columns
        try:
            print("Dropping dividends and revenue_history for clean recreate...")
            conn.execute(text("DROP TABLE IF EXISTS dividends CASCADE"))
            conn.execute(text("DROP TABLE IF EXISTS revenue_history CASCADE"))
            conn.commit()
        except Exception as e:
            print(f"Error dropping: {e}")
            conn.rollback()

    # Recreate all tables (this will pick up the new String columns in models.py)
    models.Base.metadata.create_all(bind=engine)
    print("Base.metadata.create_all executed.")
    
    # Ensure DailyKline columns are there
    cols_to_check = [
        ("dividend_per_share", "FLOAT"),
        ("dividend_yield", "FLOAT"),
        ("ex_dividend_date", "VARCHAR"),
        ("payout_ratio", "FLOAT"),
        ("dividend_growth", "FLOAT"),
        ("payout_frequency", "VARCHAR(50)"),
        ("revenue_ttm", "NUMERIC(28, 2)"),
        ("revenue_growth", "FLOAT"),
        ("revenue_per_employee", "NUMERIC(28, 2)")
    ]
    
    with engine.connect() as conn:
        for col_name, col_type in cols_to_check:
            try:
                res = conn.execute(text(f"SELECT column_name FROM information_schema.columns WHERE table_name = 'daily_klines' AND column_name = '{col_name}'")).fetchone()
                if not res:
                    print(f"Adding column {col_name} to daily_klines...")
                    conn.execute(text(f"ALTER TABLE daily_klines ADD COLUMN {col_name} {col_type}"))
                    conn.commit()
                else:
                    print(f"Column {col_name} already exists.")
            except Exception as e:
                print(f"Error adding {col_name}: {e}")
                conn.rollback()
    
    print("Migration finished.")

if __name__ == "__main__":
    migrate()
