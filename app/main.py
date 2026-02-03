from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session
from typing import List

import crud, models, schemas
from database import SessionLocal, engine, get_db

models.Base.metadata.create_all(bind=engine)

app = FastAPI()

@app.post("/stock_records/", response_model=schemas.Stock)
def create_stock(stock: schemas.StockCreate, db: Session = Depends(get_db)):
    db_stock = crud.get_stock_by_name(db, stock_name=stock.stock_name)
    if db_stock:
        raise HTTPException(status_code=400, detail="Stock already registered")
    return crud.create_stock(db=db, stock=stock)

@app.get("/stocks/", response_model=List[schemas.Stock])
def read_stocks(skip: int = 0, limit: int = 100, db: Session = Depends(get_db)):
    stocks = crud.get_stocks(db, skip=skip, limit=limit)
    return stocks

@app.get("/stocks/{stock_id}", response_model=schemas.Stock)
def read_stock(stock_id: int, db: Session = Depends(get_db)):
    db_stock = crud.get_stock(db, stock_id=stock_id)
    if db_stock is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return db_stock

@app.get("/stocks/signal", response_model = schemas.StockSignal)
def read_signal(stock_name: str, db: Session = Depends(get_db)):
    return crud.get_signal(db, stock_name=stock_name)
