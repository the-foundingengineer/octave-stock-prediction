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
