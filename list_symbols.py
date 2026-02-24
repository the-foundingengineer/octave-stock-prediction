from app.database import SessionLocal
from app.models import Stock

def check_stocks():
    db = SessionLocal()
    try:
        stocks = db.query(Stock).all()
        print(f"Total stocks: {len(stocks)}")
        for s in stocks:
            print(f"{s.symbol}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    check_stocks()
