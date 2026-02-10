from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List
from fastapi.middleware.cors import CORSMiddleware

from app.crud import create_stock_record, get_stocks, get_stock, get_market_table
from app.schemas import StockRecordCreate, StockRecord, Stock, StockCreate, MarketTableItem

from app import models
from app.database import SessionLocal, engine, get_db

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  #
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.post("/stock_records/", response_model=StockRecord)
def create_record(stock: StockRecordCreate, db: Session = Depends(get_db)):
    # You might want to check if the stock symbol exists first, 
    # but for simple record creation we'll just proceed or add a check.
    return create_stock_record(db=db, stock=stock)

@app.get("/stocks", response_model=list[Stock])
def read_stocks(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    stocks = get_stocks(db, page=page, limit=limit)
    return stocks

@app.get("/stocks/{stock_id}", response_model=Stock)
def read_stock(stock_id: int, db: Session = Depends(get_db)):
    db_stock = get_stock(db, stock_id=stock_id)
    if db_stock is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return db_stock

@app.get("/market", response_model=list[MarketTableItem])
def read_market_table(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db)
):
    return get_market_table(db, page=page, limit=limit)
