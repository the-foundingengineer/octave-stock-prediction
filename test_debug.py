import traceback
import sys

print("Step 1: basic imports", flush=True)
try:
    from app.database import SessionLocal
    print("  DB import OK", flush=True)
except Exception as e:
    print(f"  DB import FAIL: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)

print("Step 2: model import", flush=True)
try:
    from app.models import Stock, DailyKline
    print("  Model import OK", flush=True)
    print(f"  Stock columns: {[c.name for c in Stock.__table__.columns]}", flush=True)
except Exception as e:
    print(f"  Model import FAIL: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)

print("Step 3: crud import", flush=True)
try:
    from app import crud
    print("  CRUD import OK", flush=True)
except Exception as e:
    print(f"  CRUD import FAIL: {e}", flush=True)
    traceback.print_exc()
    sys.exit(1)

print("Step 4: DB query", flush=True)
try:
    db = SessionLocal()
    stocks = db.query(Stock).limit(1).all()
    print(f"  Query OK: {len(stocks)} stocks", flush=True)
    if stocks:
        s = stocks[0]
        print(f"  symbol={s.symbol}, stock_exchange={s.stock_exchange}", flush=True)
    db.close()
except Exception as e:
    print(f"  Query FAIL: {e}", flush=True)
    traceback.print_exc()

print("Step 5: crud.get_stocks", flush=True)
try:
    db = SessionLocal()
    result = crud.get_stocks(db, 1, 1)
    print(f"  get_stocks OK: {len(result)} results", flush=True)
    db.close()
except Exception as e:
    print(f"  get_stocks FAIL: {e}", flush=True)
    traceback.print_exc()

print("Step 6: crud.get_stock_stats(db, 1)", flush=True)
try:
    db = SessionLocal()
    result = crud.get_stock_stats(db, 1)
    if result:
        print(f"  get_stock_stats OK: keys={list(result.keys())}", flush=True)
    else:
        print("  get_stock_stats OK: None", flush=True)
    db.close()
except Exception as e:
    print(f"  get_stock_stats FAIL: {e}", flush=True)
    traceback.print_exc()

print("Step 7: crud.get_stock_info(db, 1)", flush=True)
try:
    db = SessionLocal()
    result = crud.get_stock_info(db, 1)
    if result:
        print(f"  get_stock_info OK: {result}", flush=True)
    else:
        print("  get_stock_info OK: None", flush=True)
    db.close()
except Exception as e:
    print(f"  get_stock_info FAIL: {e}", flush=True)
    traceback.print_exc()

print("DONE", flush=True)
