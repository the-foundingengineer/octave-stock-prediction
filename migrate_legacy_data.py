import sys
import os
from datetime import datetime, date
from decimal import Decimal
from sqlalchemy import text
from sqlalchemy.orm import Session

sys.path.append(os.path.join(os.getcwd(), "app"))

import models
from database import SessionLocal, engine

def migrate_data():
    # Ensure tables exist
    print("Initializing database tables...")
    models.Base.metadata.create_all(bind=engine)
    
    db: Session = SessionLocal()
    try:
        # 1. Fetch legacy data from 'stocks' table using raw SQL
        print("Fetching legacy data from 'stocks' table...")
        query = text("""
            SELECT id, symbol, outstanding_shares, market_cap, pe_ratio, 
                   fifty_two_week_high, fifty_two_week_low, adjustment_factor 
            FROM stocks
        """)
        res = db.execute(query)
        stocks_data = [dict(r._mapping) for r in res]
        
        migrated_bs = 0
        migrated_klines = 0
        
        for data in stocks_data:
            stock_id = data['id']
            symbol = data['symbol']
            
            try:
                # A. Migrate outstanding_shares -> BalanceSheet
                if data['outstanding_shares']:
                    bs = db.query(models.BalanceSheet).filter_by(
                        stock_id=stock_id, 
                        period_type='latest'
                    ).first()
                    
                    if not bs:
                        bs = models.BalanceSheet(
                            stock_id=stock_id,
                            period_ending=date.today(),
                            period_type='latest'
                        )
                        db.add(bs)
                    
                    try:
                        bs.shares_outstanding = int(float(data['outstanding_shares']))
                        migrated_bs += 1
                    except (ValueError, TypeError):
                        pass

                # B. Migrate other fields to the latest DailyKline
                latest_kline = db.query(models.DailyKline).filter_by(symbol=symbol).order_by(models.DailyKline.date.desc()).first()
                
                if latest_kline:
                    updated = False
                    if data['market_cap'] and latest_kline.market_cap is None:
                        try:
                            latest_kline.market_cap = Decimal(str(data['market_cap']))
                            updated = True
                        except: pass
                    
                    if data['pe_ratio'] and latest_kline.pe_ratio is None:
                        try:
                            latest_kline.pe_ratio = float(data['pe_ratio'])
                            updated = True
                        except: pass
                    
                    if data['fifty_two_week_high'] and latest_kline.week_52_high is None:
                        try:
                            latest_kline.week_52_high = float(data['fifty_two_week_high'])
                            updated = True
                        except: pass
                    
                    if data['fifty_two_week_low'] and latest_kline.week_52_low is None:
                        try:
                            latest_kline.week_52_low = float(data['fifty_two_week_low'])
                            updated = True
                        except: pass
                    
                    if data['adjustment_factor'] and latest_kline.adjustment_factor is None:
                        latest_kline.adjustment_factor = str(data['adjustment_factor'])
                        updated = True
                        
                    if updated:
                        migrated_klines += 1

                # Commit every 10 stocks to see progress and avoid massive rollback
                if migrated_bs % 10 == 0 or migrated_klines % 10 == 0:
                    db.commit()

            except Exception as e:
                print(f"Error migrating {symbol}: {e}")
                db.rollback()

        db.commit()
        print(f"Summary: Migrated {migrated_bs} Balance Sheets, {migrated_klines} Daily Klines.")
        
    except Exception as e:
        import traceback
        print(f"Fatal migration error: {e}")
        traceback.print_exc()
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    migrate_data()
