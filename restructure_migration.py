"""
Database restructuring migration script.

This script safely restructures the database by:
1. Adding stock_id to daily_klines and backfilling from stocks.symbol
2. Dropping redundant columns from daily_klines (valuation, dividends, revenue)
3. Dropping the redundant revenue_history table
4. Updating the unique constraint on daily_klines

Run with --dry-run to see what would change without making modifications.
Run without flags to apply the migration.

Usage:
    python restructure_migration.py --dry-run
    python restructure_migration.py
"""

import argparse
import sys
from datetime import datetime

from sqlalchemy import text

from app.database import SessionLocal


# Columns to remove from daily_klines
COLUMNS_TO_DROP = [
    # Valuation (already in stock_ratios)
    "market_cap",
    "enterprise_value",
    "pe_ratio",
    "forward_pe",
    "ps_ratio",
    "pb_ratio",
    # Dividend snapshots (already in dividends + stock_ratios)
    "dividend_per_share",
    "dividend_yield",
    "ex_dividend_date",
    "payout_ratio",
    "dividend_growth",
    "payout_frequency",
    # Revenue snapshots (already in income_statements)
    "revenue_ttm",
    "revenue_growth",
    "revenue_per_employee",
]


def get_row_counts(db) -> dict:
    """Get row counts for all tables."""
    tables = [
        "stocks", "daily_klines", "income_statements", "balance_sheets",
        "cash_flows", "stock_ratios", "dividends", "revenue_history",
        "stock_executives", "market_cap_history",
    ]
    counts = {}
    for t in tables:
        try:
            result = db.execute(text(f"SELECT COUNT(*) FROM {t}"))
            counts[t] = result.scalar()
        except Exception:
            # If the query fails (e.g. table doesn't exist), the transaction is aborted.
            # We must rollback to clean up the failed transaction state before continuing.
            db.rollback()
            counts[t] = "TABLE NOT FOUND"
    return counts


def get_column_names(db, table: str) -> list:
    """Get column names for a table."""
    result = db.execute(text(
        "SELECT column_name FROM information_schema.columns "
        f"WHERE table_name = '{table}' ORDER BY ordinal_position"
    ))
    return [row[0] for row in result]


def check_column_exists(db, table: str, column: str) -> bool:
    """Check if a column exists on a table."""
    result = db.execute(text(
        "SELECT COUNT(*) FROM information_schema.columns "
        f"WHERE table_name = '{table}' AND column_name = '{column}'"
    ))
    return result.scalar() > 0


def check_constraint_exists(db, constraint_name: str) -> bool:
    """Check if a constraint exists."""
    result = db.execute(text(
        "SELECT COUNT(*) FROM information_schema.table_constraints "
        f"WHERE constraint_name = '{constraint_name}'"
    ))
    return result.scalar() > 0


def check_table_exists(db, table: str) -> bool:
    """Check if a table exists."""
    result = db.execute(text(
        "SELECT COUNT(*) FROM information_schema.tables "
        f"WHERE table_name = '{table}'"
    ))
    return result.scalar() > 0


def run_migration(dry_run: bool = False):
    db = SessionLocal()
    try:
        print(f"\n{'='*60}")
        print(f"  Database Restructuring Migration")
        print(f"  {'DRY RUN' if dry_run else 'LIVE RUN'}")
        print(f"  Started: {datetime.now()}")
        print(f"{'='*60}\n")

        # ── Step 0: Print before counts ────────────────────────────────
        print("📊 Row counts BEFORE migration:")
        before_counts = get_row_counts(db)
        for table, count in before_counts.items():
            print(f"   {table:25s} → {count}")

        print(f"\n📋 Current daily_klines columns:")
        dk_cols = get_column_names(db, "daily_klines")
        print(f"   {len(dk_cols)} columns: {', '.join(dk_cols)}")

        # ── Step 1: Add stock_id column ────────────────────────────────
        print(f"\n{'─'*60}")
        print("Step 1: Add stock_id column to daily_klines")

        if check_column_exists(db, "daily_klines", "stock_id"):
            print("   ✅ stock_id column already exists, skipping.")
        else:
            print("   Adding stock_id column...")
            if not dry_run:
                db.execute(text(
                    "ALTER TABLE daily_klines "
                    "ADD COLUMN stock_id INTEGER"
                ))
                db.commit()
            print("   ✅ Added stock_id column.")

        # ── Step 2: Backfill stock_id from stocks.symbol ───────────────
        print(f"\n{'─'*60}")
        print("Step 2: Backfill stock_id from stocks.symbol join")

        if not dry_run:
            result = db.execute(text(
                "UPDATE daily_klines dk "
                "SET stock_id = s.id "
                "FROM stocks s "
                "WHERE UPPER(dk.symbol) = UPPER(s.symbol) "
                "AND dk.stock_id IS NULL"
            ))
            db.commit()
            print(f"   ✅ Backfilled {result.rowcount} rows.")
        else:
            result = db.execute(text(
                "SELECT COUNT(*) FROM daily_klines WHERE stock_id IS NULL"
            ))
            null_count = result.scalar()
            print(f"   Would backfill {null_count} rows.")

        # Check for orphaned rows (no matching stock)
        orphan_result = db.execute(text(
            "SELECT COUNT(*) FROM daily_klines dk "
            "WHERE NOT EXISTS (SELECT 1 FROM stocks s WHERE UPPER(dk.symbol) = UPPER(s.symbol))"
        ))
        orphan_count = orphan_result.scalar()
        if orphan_count > 0:
            print(f"   ⚠️  {orphan_count} orphaned kline rows (no matching stock). These will be deleted.")
            if not dry_run:
                db.execute(text(
                    "DELETE FROM daily_klines dk "
                    "WHERE NOT EXISTS (SELECT 1 FROM stocks s WHERE UPPER(dk.symbol) = UPPER(s.symbol))"
                ))
                db.commit()
                print(f"   ✅ Deleted {orphan_count} orphaned rows.")

        # ── Step 3: Add FK constraint and NOT NULL ─────────────────────
        print(f"\n{'─'*60}")
        print("Step 3: Add foreign key constraint and set NOT NULL")

        if not dry_run:
            # Set NOT NULL
            db.execute(text(
                "ALTER TABLE daily_klines "
                "ALTER COLUMN stock_id SET NOT NULL"
            ))
            # Add FK constraint
            if not check_constraint_exists(db, "fk_dailyklines_stock_id"):
                db.execute(text(
                    "ALTER TABLE daily_klines "
                    "ADD CONSTRAINT fk_dailyklines_stock_id "
                    "FOREIGN KEY (stock_id) REFERENCES stocks(id)"
                ))
            # Add index on stock_id
            db.execute(text(
                "CREATE INDEX IF NOT EXISTS ix_daily_klines_stock_id "
                "ON daily_klines (stock_id)"
            ))
            db.commit()
            print("   ✅ FK constraint, NOT NULL, and index added.")
        else:
            print("   Would add FK constraint, NOT NULL, and index.")

        # ── Step 4: Drop old unique constraint and symbol column ───────
        print(f"\n{'─'*60}")
        print("Step 4: Replace unique constraint and drop symbol column")

        if not dry_run:
            # Drop old unique constraint
            if check_constraint_exists(db, "uix_symbol_date"):
                db.execute(text(
                    "ALTER TABLE daily_klines DROP CONSTRAINT uix_symbol_date"
                ))
                print("   ✅ Dropped old uix_symbol_date constraint.")

            # Add new unique constraint
            if not check_constraint_exists(db, "uix_stock_id_date"):
                db.execute(text(
                    "ALTER TABLE daily_klines "
                    "ADD CONSTRAINT uix_stock_id_date UNIQUE (stock_id, date)"
                ))
                print("   ✅ Added new uix_stock_id_date constraint.")

            # Drop old symbol column
            if check_column_exists(db, "daily_klines", "symbol"):
                db.execute(text(
                    "ALTER TABLE daily_klines DROP COLUMN symbol"
                ))
                print("   ✅ Dropped symbol column.")

            db.commit()
        else:
            print("   Would replace uix_symbol_date → uix_stock_id_date")
            print("   Would drop symbol column")

        # ── Step 5: Drop redundant columns ─────────────────────────────
        print(f"\n{'─'*60}")
        print("Step 5: Drop redundant columns from daily_klines")

        dropped = []
        for col in COLUMNS_TO_DROP:
            if check_column_exists(db, "daily_klines", col):
                if not dry_run:
                    db.execute(text(
                        f"ALTER TABLE daily_klines DROP COLUMN {col}"
                    ))
                dropped.append(col)
            else:
                pass  # Already gone

        if not dry_run:
            db.commit()

        if dropped:
            print(f"   {'Would drop' if dry_run else '✅ Dropped'} {len(dropped)} columns:")
            for col in dropped:
                print(f"      - {col}")
        else:
            print("   ✅ All redundant columns already removed.")

        # ── Step 6: Drop revenue_history table ─────────────────────────
        print(f"\n{'─'*60}")
        print("Step 6: Drop redundant revenue_history table")

        if check_table_exists(db, "revenue_history"):
            rev_count = db.execute(text("SELECT COUNT(*) FROM revenue_history")).scalar()
            print(f"   revenue_history has {rev_count} rows.")
            if not dry_run:
                db.execute(text("DROP TABLE revenue_history"))
                db.commit()
                print("   ✅ Dropped revenue_history table.")
            else:
                print("   Would drop revenue_history table.")
        else:
            print("   ✅ revenue_history table already dropped.")

        # ── Step 7: Print after counts ─────────────────────────────────
        print(f"\n{'─'*60}")
        print("📊 Row counts AFTER migration:")
        after_counts = get_row_counts(db)
        for table, count in after_counts.items():
            before = before_counts.get(table, "N/A")
            indicator = ""
            if isinstance(count, int) and isinstance(before, int):
                if count != before:
                    indicator = f" (was {before})"
            elif count == "TABLE NOT FOUND":
                indicator = " (DROPPED)"
            print(f"   {table:25s} → {count}{indicator}")

        print(f"\n📋 Final daily_klines columns:")
        dk_cols_after = get_column_names(db, "daily_klines")
        print(f"   {len(dk_cols_after)} columns: {', '.join(dk_cols_after)}")

        print(f"\n{'='*60}")
        print(f"  Migration {'preview' if dry_run else 'completed'} successfully!")
        print(f"  Finished: {datetime.now()}")
        print(f"{'='*60}\n")

    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        db.rollback()
        raise
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Database restructuring migration")
    parser.add_argument("--dry-run", action="store_true",
                        help="Preview changes without applying them")
    args = parser.parse_args()

    if not args.dry_run:
        print("\n⚠️  This will modify your database!")
        print("   Run with --dry-run first to preview changes.")
        confirm = input("   Type 'yes' to proceed: ")
        if confirm.lower() != "yes":
            print("   Aborted.")
            sys.exit(0)

    run_migration(dry_run=args.dry_run)
