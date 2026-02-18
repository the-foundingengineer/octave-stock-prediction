from app.crud import get_stock_related
from app.schemas import StockRelatedResponse
from fastapi import Depends, FastAPI, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List
from fastapi.middleware.cors import CORSMiddleware

from app.crud import create_stock_record, get_stocks, get_stock, get_stock_kline, get_stock_stats, get_stock_info, get_stock_by_income_statement
from app.schemas import StockRecordCreate, StockRecord, Stock, KlineResponse, StockStatsResponse, StockInfoResponse, IncomeStatementResponse
from app.crud import create_stock_record, get_stocks, get_stock, get_stock_kline, get_stock_stats, get_stock_info, get_popular_comparisons, get_stock_comparison_details
from app.schemas import StockRecordCreate, StockRecord, Stock, KlineResponse, StockStatsResponse, StockInfoResponse, PopularComparisonResponse, StockComparisonItem

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

@app.get("/stocks/{stock_id}/financials/income-statement", response_model=IncomeStatementResponse)
def get_income_statement(stock_id: int, db: Session = Depends(get_db)):
    db_stock_with_income = get_stock_by_income_statement(db, stock_id)
    if db_stock_with_income is None:
        raise HTTPException(status_code=404, detail="Stock or income statement not found")
    
    # Extract the latest income statement
    income_stmt = db_stock_with_income.income_stmts[0] if db_stock_with_income.income_stmts else None
    
    return {
        "id": db_stock_with_income.id,
        "symbol": db_stock_with_income.symbol,
        "name": db_stock_with_income.name,
        "sector": db_stock_with_income.sector,
        "industry": db_stock_with_income.industry,
        "exchange": db_stock_with_income.exchange,
        "currency": db_stock_with_income.currency,
        "country": db_stock_with_income.country,
        "website": db_stock_with_income.website,
        "ceo": db_stock_with_income.ceo,
        "employees": db_stock_with_income.employees,
        "fiscal_year_end": db_stock_with_income.fiscal_year_end,
        "income_statement": {
            "id": income_stmt.id,
            "stock_id": income_stmt.stock_id,
            "period_ending": str(income_stmt.period_ending),
            "period_type": income_stmt.period_type,
            "revenue": float(income_stmt.revenue) if income_stmt.revenue else None,
            "operating_revenue": float(income_stmt.operating_revenue) if income_stmt.operating_revenue else None,
            "other_revenue": float(income_stmt.other_revenue) if income_stmt.other_revenue else None,
            "revenue_growth_yoy": float(income_stmt.revenue_growth_yoy) if income_stmt.revenue_growth_yoy else None,
            "cost_of_revenue": float(income_stmt.cost_of_revenue) if income_stmt.cost_of_revenue else None,
            "gross_profit": float(income_stmt.gross_profit) if income_stmt.gross_profit else None,
            "sga_expenses": float(income_stmt.sga_expenses) if income_stmt.sga_expenses else None,
            "operating_income": float(income_stmt.operating_income) if income_stmt.operating_income else None,
            "ebitda": float(income_stmt.ebitda) if income_stmt.ebitda else None,
            "ebit": float(income_stmt.ebit) if income_stmt.ebit else None,
            "interest_expense": float(income_stmt.interest_expense) if income_stmt.interest_expense else None,
            "pretax_income": float(income_stmt.pretax_income) if income_stmt.pretax_income else None,
            "income_tax": float(income_stmt.income_tax) if income_stmt.income_tax else None,
            "net_income": float(income_stmt.net_income) if income_stmt.net_income else None,
            "net_income_growth_yoy": float(income_stmt.net_income_growth_yoy) if income_stmt.net_income_growth_yoy else None,
            "eps_basic": float(income_stmt.eps_basic) if income_stmt.eps_basic else None,
            "eps_diluted": float(income_stmt.eps_diluted) if income_stmt.eps_diluted else None,
            "eps_growth_yoy": float(income_stmt.eps_growth_yoy) if income_stmt.eps_growth_yoy else None,
            "dividend_per_share": float(income_stmt.dividend_per_share) if income_stmt.dividend_per_share else None,
            "shares_basic": income_stmt.shares_basic,
            "shares_diluted": income_stmt.shares_diluted,
        } if income_stmt else None
    }
@app.get("/stocks/popular_comparisons", response_model=PopularComparisonResponse)
def read_popular_comparisons(db: Session = Depends(get_db)):
    db_popular_comparisons = get_popular_comparisons(db)
    if not db_popular_comparisons:
        raise HTTPException(status_code=404, detail="Stock popular comparisons not found")
    return {"stocks": db_popular_comparisons}

@app.get("/stocks/{stock_id}/comparison", response_model=StockComparisonItem)
def get_comparison_details(stock_id: int, db: Session = Depends(get_db)):
    db_details = get_stock_comparison_details(db, stock_id)
    if not db_details:
        raise HTTPException(status_code=404, detail="Stock comparison details not found")
    return db_details
