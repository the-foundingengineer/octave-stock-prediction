from app.crud import get_stock_related
from app.schemas import StockRelatedResponse
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List
from fastapi.middleware.cors import CORSMiddleware

from app.crud import create_stock_record, get_stocks, get_stock, get_stock_kline, get_stock_stats, get_stock_info
from app.schemas import StockRecordCreate, StockRecord, Stock, KlineResponse, StockStatsResponse, StockInfoResponse

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

@app.post("/stocks/{symbol}/refresh")
def refresh_stock(symbol: str, token: str, db: Session = Depends(get_db)):
    try:
        from app.services import update_stock_info
        stock = update_stock_info(db, symbol, token)
        return stock
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@app.get("/stocks/{stock_id}/klines", response_model=KlineResponse)
def get_klines(stock_id: int, interval: str, limit:int = Query(500, ge=1, le=1000), db: Session = Depends(get_db)):
    db_stock_kline = get_stock_kline(db, stock_id, interval, limit)
    if db_stock_kline is None:
        raise HTTPException(status_code=404, detail="Stock kline not found")
    return db_stock_kline

@app.get("/stocks/{stock_id}/stats", response_model=StockStatsResponse)
def get_stats(stock_id: int, db: Session = Depends(get_db)):
    db_stats = get_stock_stats(db, stock_id)
    if db_stats is None:
        raise HTTPException(status_code=404, detail="Stock statistics not found")
    return db_stats

@app.get("/stocks/{stock_id}/info", response_model=StockInfoResponse)
def get_info(stock_id: int, db: Session = Depends(get_db)):
    db_info = get_stock_info(db, stock_id)
    if db_info is None:
        raise HTTPException(status_code=404, detail="Stock info not found")
    return db_info

@app.get("/stocks/{stock_id}/related", response_model=list[StockRelatedResponse])
def get_related(stock_id: int, limit: int = 10, db: Session = Depends(get_db)):
    db_related = get_stock_related(db, stock_id, limit)
    if db_related is None:
        raise HTTPException(status_code=404, detail="Stock related not found")
    return db_related

