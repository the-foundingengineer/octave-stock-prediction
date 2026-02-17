"""
FastAPI application entry-point for the Octave stock prediction API.

Endpoints:
    POST /stock_records/                           – Create a daily kline record
    GET  /stocks                                   – Paginated stock list
    GET  /stocks/search?q={query}                  – Search stocks by symbol/name
    GET  /stocks/compare?symbols=A,B&interval=week – Bulk comparison
    GET  /stocks/{stock_id}                        – Single stock detail
    GET  /stocks/{stock_id}/klines                 – Aggregated OHLCV
    GET  /stocks/{stock_id}/stats                  – Comprehensive statistics
    GET  /stocks/{stock_id}/info                   – Profile + technicals
    GET  /stocks/{stock_id}/related                – Same-sector stocks
    GET  /stocks/{stock_id}/financials/income-statement
    GET  /stocks/{stock_id}/comparison             – Full comparison data
    POST /stocks/{symbol}/refresh                  – Refresh from iTick API
    GET  /popular_comparisons                      – Top stocks per sector
"""

from typing import List

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy.orm import Session

from app import models
from app.database import SessionLocal, engine, get_db
from app.crud import (
    create_stock_record,
    format_income_statement,
    get_bulk_comparison,
    get_popular_comparisons,
    get_stock,
    get_stock_by_income_statement,
    get_stock_comparison_details,
    get_stock_dividends,
    get_stock_info,
    get_stock_kline,
    get_stock_related,
    get_stock_stats,
    get_stocks,
    search_stocks,
)
from app.schemas import (
    BulkComparisonResponse,
    DividendResponse,
    KlineResponse,
    PopularComparisonResponse,
    Stock,
    StockComparisonItem,
    StockInfoResponse,
    StockRecord,
    StockRecordCreate,
    StockRelatedResponse,
    StockSearchResult,
    StockStatsResponse,
    StockWithIncomeStatementResponse,
)
from app.services import update_stock_info

# ── App setup ────────────────────────────────────────────────────────────────

models.Base.metadata.create_all(bind=engine)

app = FastAPI(
    title="Octave Stock API",
    description="REST API for Nigerian stock market data and analysis.",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ── Stock records ────────────────────────────────────────────────────────────


@app.post("/stock_records/", response_model=StockRecord)
def create_record(stock: StockRecordCreate, db: Session = Depends(get_db)):
    """Create a new stock record."""
    return create_stock_record(db=db, stock=stock)


# ── Stock listing & detail ───────────────────────────────────────────────────


@app.get("/stocks", response_model=List[Stock])
def read_stocks(
    page: int = Query(1, ge=1),
    limit: int = Query(10, ge=1, le=100),
    db: Session = Depends(get_db),
):
    """Return a paginated list of stocks."""
    return get_stocks(db, page=page, limit=limit)


@app.get("/stocks/search", response_model=List[StockSearchResult])
def stock_search(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(10, ge=1, le=50),
    db: Session = Depends(get_db),
):
    """Search stocks by symbol or company name."""
    return search_stocks(db, query=q, limit=limit)


@app.get("/stocks/compare", response_model=BulkComparisonResponse)
def bulk_compare(
    symbols: str = Query(..., description="Comma-separated stock symbols"),
    interval: str = Query("week", description="Kline interval: day, week, month, year"),
    limit: int = Query(52, ge=1, le=500, description="Max klines per stock"),
    db: Session = Depends(get_db),
):
    """Fetch klines and stats for multiple stocks in one request."""
    symbol_list = [s.strip() for s in symbols.split(",") if s.strip()]
    if not symbol_list:
        raise HTTPException(status_code=400, detail="No symbols provided")
    comparisons = get_bulk_comparison(db, symbol_list, interval, limit)
    return {"comparisons": comparisons}


@app.get("/stocks/{stock_id}", response_model=Stock)
def read_stock(stock_id: int, db: Session = Depends(get_db)):
    """Return a single stock by ID."""
    db_stock = get_stock(db, stock_id=stock_id)
    if db_stock is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return db_stock


# ── Klines ───────────────────────────────────────────────────────────────────


@app.get("/stocks/{stock_id}/klines", response_model=KlineResponse)
def get_klines(
    stock_id: int,
    interval: str,
    limit: int = Query(500, ge=1, le=1000),
    db: Session = Depends(get_db),
):
    """Return aggregated OHLCV klines for a stock."""
    result = get_stock_kline(db, stock_id, interval, limit)
    if result is None:
        raise HTTPException(status_code=404, detail="Stock kline not found")
    return result


# ── Stats & info ─────────────────────────────────────────────────────────────


@app.get("/stocks/{stock_id}/stats", response_model=StockStatsResponse)
def get_stats(stock_id: int, db: Session = Depends(get_db)):
    """Return comprehensive statistics for a stock."""
    result = get_stock_stats(db, stock_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Stock statistics not found")
    return result


@app.get("/stocks/{stock_id}/info", response_model=StockInfoResponse)
def get_info(stock_id: int, db: Session = Depends(get_db)):
    """Return profile and technical info for a stock."""
    result = get_stock_info(db, stock_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Stock info not found")
    return result


# ── Related stocks ───────────────────────────────────────────────────────────


@app.get("/stocks/{stock_id}/related", response_model=List[StockRelatedResponse])
def get_related(
    stock_id: int,
    limit: int = 10,
    db: Session = Depends(get_db),
):
    """Return stocks in the same sector."""
    result = get_stock_related(db, stock_id, limit)
    if result is None:
        raise HTTPException(status_code=404, detail="Stock related not found")
    return result


# ── Dividends ────────────────────────────────────────────────────────────────


@app.get("/stocks/{stock_id}/dividends", response_model=List[DividendResponse])
def get_dividends(stock_id: int, db: Session = Depends(get_db)):
    """Return historical dividends for a stock."""
    result = get_stock_dividends(db, stock_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Stock not found")
    return result


# ── Financials ───────────────────────────────────────────────────────────────


@app.get(
    "/stocks/{stock_id}/financials/income-statement",
    response_model=StockWithIncomeStatementResponse,
)
def get_income_statement(stock_id: int, db: Session = Depends(get_db)):
    """Return the stock profile with its latest income statement."""
    db_stock = get_stock_by_income_statement(db, stock_id)
    if db_stock is None:
        raise HTTPException(status_code=404, detail="Stock or income statement not found")

    latest_stmt = db_stock.income_stmts[0] if db_stock.income_stmts else None

    return {
        "id": db_stock.id,
        "symbol": db_stock.symbol,
        "name": db_stock.name,
        "sector": db_stock.sector,
        "industry": db_stock.industry,
        "exchange": db_stock.exchange,
        "currency": db_stock.currency,
        "country": db_stock.country,
        "website": db_stock.website,
        "ceo": db_stock.ceo,
        "employees": db_stock.employees,
        "fiscal_year_end": db_stock.fiscal_year_end,
        "income_statement": format_income_statement(latest_stmt),
    }


# ── Comparisons ──────────────────────────────────────────────────────────────


@app.get("/popular_comparisons", response_model=PopularComparisonResponse)
def read_popular_comparisons(db: Session = Depends(get_db)):
    """Return the top stocks per sector for comparison."""
    results = get_popular_comparisons(db)
    if not results:
        raise HTTPException(status_code=404, detail="Stock popular comparisons not found")
    return {"stocks": results}


@app.get("/stocks/{stock_id}/comparison", response_model=StockComparisonItem)
def get_comparison_details(stock_id: int, db: Session = Depends(get_db)):
    """Return full comparison data for a single stock."""
    result = get_stock_comparison_details(db, stock_id)
    if not result:
        raise HTTPException(status_code=404, detail="Stock comparison details not found")
    return result


# ── External refresh ─────────────────────────────────────────────────────────


@app.post("/stocks/{symbol}/refresh")
def refresh_stock(symbol: str, token: str, db: Session = Depends(get_db)):
    """Refresh stock data from the iTick API."""
    try:
        stock = update_stock_info(db, symbol, token)
        return stock
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
