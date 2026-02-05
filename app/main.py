from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
from typing import List
from fastapi.middleware.cors import CORSMiddleware

from app.crud import get_stock_by_name, create_stock, get_stocks, get_stock, get_signal, get_unique_stock_names

from app.schemas import StockCreate, StockSignal,Stock, StockBase, unique_name

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

@app.post("/stock_records/", response_model=Stock)
def create_stock(stock: StockCreate, db: Session = Depends(get_db)):
    db_stock = get_stock_by_name(db, stock_name=stock.stock_name)
    if db_stock:
        raise HTTPException(status_code=400, detail="Stock already registered")
    return create_stock(db=db, stock=stock)

@app.get("/stocks/", response_model=List[Stock])
def read_stocks(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    stocks = get_stocks(db, skip=skip, limit=limit)
    return stocks

@app.get("/stocks/signal", response_model = StockSignal)
def read_signal(stock_name: str, db: Session = Depends(get_db)):
    return get_signal( stock_name=stock_name, db=db)

@app.get("/stocks/stocks_name", response_model=List[unique_name])
def get_unique_stock(db: Session = Depends(get_db)):
    # db_stock = get_unique_stock_names(db)
    # if db_stock is None:
    #     raise HTTPException(status_code=404, detail="Stock not found")
    return get_unique_stock_names(db)

@app.get("/stocks/{stock_id}", response_model=Stock)
def read_stock(stock_id: int, db: Session = Depends(get_db)):
    db_stock = get_stock(db, stock_id=stock_id)
    if db_stock is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return db_stock


@app.get("/stocks/name/{stock_name}", response_model=List[StockBase])
def read_stock_name(stock_name: str, db: Session = Depends(get_db)):
    db_stock = get_stock_by_name(db, stock_name=stock_name)
    if db_stock is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return db_stock

