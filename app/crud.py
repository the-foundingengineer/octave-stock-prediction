from sqlalchemy.orm import Session
import models, schemas

def get_stock(db: Session, stock_id: int):
    return db.query(models.StockRecord).filter(models.StockRecord.id == stock_id).first()

def get_stock_by_name(db: Session, stock_name: str):
    return db.query(models.StockRecord).filter(models.StockRecord.stock_name == stock_name).first()

def get_stocks(db: Session, skip: int = 0, limit: int = 100):
    return db.query(models.StockRecord).offset(skip).limit(limit).all()

def create_stock(db: Session, stock: schemas.StockCreate):
    db_stock = models.StockRecord(date=stock.date, open=stock.open, high=stock.high, low=stock.low, close=stock.close, volume=stock.volume, stock_name=stock.stock_name)
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock

def get_signal(db: Session, stock_name: str):
    df = get_stock_by_name(db, stock_name=stock_name)
    if df is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    df["ma_5"] = df["close_price"].rolling(5).mean()
    df["ma_20"] = df["close_price"].rolling(20).mean()

    latest = df.iloc[-1]
    score = 0
    reasons = []

    # Trend
    if latest["close_price"] > latest["ma_20"]:
        score += 1
        reasons.append("Price above 20-day moving average")
    else:
        score -= 1
        reasons.append("Price below 20-day moving average")

    # Momentum
    if latest["ma_5"] > latest["ma_20"]:
        score += 1
        reasons.append("Positive short-term momentum")
    else:
        score -= 1
        reasons.append("Negative short-term momentum")

    # Volume confirmation
    avg_volume = df["volume"].rolling(20).mean().iloc[-1]
    if latest["volume"] > avg_volume:
        score += 1
        reasons.append("Above-average trading volume")
    else:
        score -= 1
        reasons.append("Below-average trading volume")

    # Signal mapping
    if score >= 3:
        signal = "Strong Buy"
    elif score >= 1:
        signal = "Buy"
    elif score == 0:
        signal = "Neutral"
    elif score <= -3:
        signal = "Strong Sell"
    else:
        signal = "Sell"

    return {
        "symbol": symbol,
        "signal": signal,
        "score": score,
        "reasons": reasons
    }

