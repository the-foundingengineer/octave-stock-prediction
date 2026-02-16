import sys
import os
from sqlalchemy import text
sys.path.append(os.path.join(os.getcwd(), "app"))
from database import engine

def update_schema():
    with engine.begin() as conn:
        print("Updating 'daily_klines' table...")
        dk_columns = [
            ("week_52_high", "FLOAT"),
            ("week_52_low", "FLOAT"),
            ("avg_volume_20d", "BIGINT"),
            ("rsi", "FLOAT"),
            ("ma_50d", "FLOAT"),
            ("ma_200d", "FLOAT"),
            ("beta", "FLOAT"),
            ("market_cap", "NUMERIC(28, 2)"),
            ("enterprise_value", "NUMERIC(28, 2)"),
            ("pe_ratio", "FLOAT"),
            ("forward_pe", "FLOAT"),
            ("ps_ratio", "FLOAT"),
            ("pb_ratio", "FLOAT"),
            ("dividend_per_share", "FLOAT"),
            ("dividend_yield", "FLOAT"),
            ("ex_dividend_date", "VARCHAR"),
            ("adjustment_factor", "VARCHAR")
        ]
        
        for col_name, col_type in dk_columns:
            try:
                # Use a separate sub-transaction-like approach or just ignore errors if it exists
                # In PostgreSQL, you can check if column exists first or just catch the exception
                query = text(f"ALTER TABLE daily_klines ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                conn.execute(query)
                print(f"  Processed {col_name} in daily_klines")
            except Exception as e:
                print(f"  Error adding {col_name} to daily_klines: {e}")
        
        print("\nUpdating 'stocks' table...")
        stocks_columns = [
            ("exchange", "VARCHAR(50)"),
            ("currency", "VARCHAR(10)"),
            ("description", "TEXT"),
            ("website", "VARCHAR(200)"),
            ("country", "VARCHAR(100)"),
            ("founded", "VARCHAR(10)"),
            ("ceo", "VARCHAR(100)"),
            ("employees", "INTEGER"),
            ("fiscal_year_end", "VARCHAR(20)"),
            ("sic_code", "VARCHAR(10)"),
            ("reporting_currency", "VARCHAR(10)"),
            ("last_updated", "TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        ]
        
        for col_name, col_type in stocks_columns:
            try:
                query = text(f"ALTER TABLE stocks ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                conn.execute(query)
                print(f"  Processed {col_name} in stocks")
            except Exception as e:
                print(f"  Error adding {col_name} to stocks: {e}")
        
        print("\nSchema update complete.")

if __name__ == "__main__":
    update_schema()
