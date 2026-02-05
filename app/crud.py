from sqlalchemy.orm import Session
from app.models import StockRecord
from app.schemas import StockCreate
from fastapi import HTTPException
import pandas as pd

def get_stock(db: Session, stock_id: int):
    return db.query(StockRecord).filter(StockRecord.id == stock_id).first()
def get_stock_by_name(db: Session, stock_name: str):
    return db.query(StockRecord).filter(StockRecord.stock_name == stock_name).all()

def get_stocks(db: Session, skip: int = 0, limit: int = 100):
    return db.query(StockRecord).offset(skip).limit(limit).all()

def get_unique_stock_names(db: Session):
    results =  (
        db.query(StockRecord.stock_name)
        .distinct()
        .all()
    )
    return [{"stock_name": row[0].upper()} for row in results]

def create_stock(db: Session, stock: StockCreate):
    db_stock = StockRecord(date=stock.date, open=stock.open, high=stock.high, low=stock.low, close=stock.close, volume=stock.volume, stock_name=stock.stock_name)
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock

def get_signal(db: Session, stock_name: str):
    records = get_stock_by_name(db, stock_name=stock_name)
    if records is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    
    df = pd.DataFrame([{
    "date": r.date,
    "close": float(r.close),
    "volume": float(r.volume)
} for r in records])
    
    df["ma_5"] = df["close"].rolling(5).mean()
    df["ma_20"] = df["close"].rolling(20).mean()

    latest = df.iloc[-1]
    score = 0
    reasons = []

    # Trend
    if latest["close"] > latest["ma_20"]:
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
        "symbol": stock_name,
        "signal": signal,
        "score": score,
        "reasons": reasons
    }