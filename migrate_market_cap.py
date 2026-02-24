"""
Migration script: Create market_cap_history table and backfill from existing data.

Backfill sources:
  1. daily_klines  – rows with non-null market_cap → frequency = 'daily'
  2. stock_ratios  – rows with non-null market_cap → frequency = 'annual'
     (only if no daily record already exists for that date)
"""

from datetime import datetime
from decimal import Decimal

from app.database import engine, SessionLocal, Base
from app.models import DailyKline, MarketCapHistory, Stock, StockRatio

def run_migration():
    print(f"[{datetime.now()}] Creating market_cap_history table if not exists...")

    # Create only the new table (won't touch existing ones)
    MarketCapHistory.__table__.create(bind=engine, checkfirst=True)
    print("  ✅ Table created (or already existed).")

    db = SessionLocal()
    try:
        # ── Backfill from daily_klines ────────────────────────────────────────
        print("\n── Backfilling from daily_klines ──")
        kline_rows = (
            db.query(DailyKline)
            .filter(DailyKline.market_cap.isnot(None))
            .all()
        )
        print(f"  Found {len(kline_rows)} kline rows with market_cap data.")

        # Get stock symbol → id mapping
        stocks = {s.symbol: s.id for s in db.query(Stock).all()}
        
        inserted_daily = 0
        skipped_daily = 0
        for kline in kline_rows:
            stock_id = stocks.get(kline.symbol)
            if not stock_id:
                continue
            
            existing = (
                db.query(MarketCapHistory)
                .filter(
                    MarketCapHistory.stock_id == stock_id,
                    MarketCapHistory.date == kline.date,
                )
                .first()
            )
            if existing:
                skipped_daily += 1
                continue

            db.add(MarketCapHistory(
                stock_id=stock_id,
                date=kline.date,
                market_cap=kline.market_cap,
                frequency="daily",
            ))
            inserted_daily += 1

            # Batch commit every 500 rows
            if inserted_daily % 500 == 0:
                db.commit()
                print(f"    ... committed {inserted_daily} rows so far")

        db.commit()
        print(f"  ✅ Inserted {inserted_daily} daily rows (skipped {skipped_daily} duplicates).")

        # ── Backfill from stock_ratios ────────────────────────────────────────
        print("\n── Backfilling from stock_ratios ──")
        ratio_rows = (
            db.query(StockRatio)
            .filter(StockRatio.market_cap.isnot(None))
            .all()
        )
        print(f"  Found {len(ratio_rows)} ratio rows with market_cap data.")

        inserted_annual = 0
        skipped_annual = 0
        for ratio in ratio_rows:
            date_str = str(ratio.period_ending) if ratio.period_ending else None
            if not date_str:
                continue
            
            existing = (
                db.query(MarketCapHistory)
                .filter(
                    MarketCapHistory.stock_id == ratio.stock_id,
                    MarketCapHistory.date == date_str,
                )
                .first()
            )
            if existing:
                skipped_annual += 1
                continue

            db.add(MarketCapHistory(
                stock_id=ratio.stock_id,
                date=date_str,
                market_cap=ratio.market_cap,
                frequency="annual",
            ))
            inserted_annual += 1

        db.commit()
        print(f"  ✅ Inserted {inserted_annual} annual rows (skipped {skipped_annual} duplicates).")

        # ── Summary ──────────────────────────────────────────────────────────
        total = db.query(MarketCapHistory).count()
        print(f"\n🏁 Migration complete. Total rows in market_cap_history: {total}")

    except Exception as e:
        db.rollback()
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        db.close()


if __name__ == "__main__":
    run_migration()
