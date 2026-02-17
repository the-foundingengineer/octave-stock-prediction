"""
CRUD operations and business logic for the Octave stock API.

Public functions (used by route handlers in main.py):
    - get_stock / get_stocks           : Basic stock lookups
    - create_stock_record              : Insert a daily kline
    - get_stock_kline                  : Aggregated OHLCV by interval
    - get_stock_stats                  : Comprehensive stock statistics
    - get_stock_info                   : Lightweight profile + technicals
    - get_stock_related                : Same-sector stocks
    - get_stock_by_income_statement    : Stock with latest income statement
    - get_popular_comparisons          : Top-2 stocks per sector by market cap
    - get_stock_comparison_details     : Full comparison data for one stock
    - search_stocks                    : Search by symbol or name
    - get_bulk_comparison              : Klines + stats for multiple symbols
"""

from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from app.models import (
    BalanceSheet, CashFlow, DailyKline,
    Dividend, IncomeStatement, Stock, StockRatio,
)
from app.schemas import StockRecordCreate


# ════════════════════════════════════════════════════════════════════════════
#  Private helpers
# ════════════════════════════════════════════════════════════════════════════


def _safe_float(val) -> float:
    """Convert *val* to float, returning 0.0 on failure."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def _optional_float(val) -> Optional[float]:
    """Convert *val* to float if truthy, otherwise return None."""
    return float(val) if val else None


def _get_latest_klines(db: Session, symbol: str, limit: int = 2) -> List[DailyKline]:
    """
    Fetch the latest *limit* daily klines for *symbol*, ordered newest-first.
    Used by stats, info, and comparison helpers.
    """
    return (
        db.query(DailyKline)
        .filter(DailyKline.symbol == symbol)
        .order_by(desc(DailyKline.date))
        .limit(limit)
        .all()
    )


def _resolve_stock(db: Session, stock_id: int) -> Optional[Stock]:
    """Look up a Stock by primary key, or return None."""
    return db.query(Stock).filter(Stock.id == stock_id).first()


# ════════════════════════════════════════════════════════════════════════════
#  Basic stock operations
# ════════════════════════════════════════════════════════════════════════════


def get_stock(db: Session, stock_id: int) -> Optional[Stock]:
    """Return a single stock by ID."""
    return _resolve_stock(db, stock_id)


def get_stocks(db: Session, page: int, limit: int) -> List[Stock]:
    """Return a paginated list of stocks ordered by ID."""
    offset = (page - 1) * limit
    return (
        db.query(Stock)
        .order_by(Stock.id)
        .offset(offset)
        .limit(limit)
        .all()
    )


def create_stock_record(db: Session, stock: StockRecordCreate) -> DailyKline:
    """Insert a new daily kline record."""
    db_stock = DailyKline(
        date=stock.date,
        open=stock.open,
        high=stock.high,
        low=stock.low,
        close=stock.close,
        volume=stock.volume,
        symbol=stock.symbol,
    )
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock


# ════════════════════════════════════════════════════════════════════════════
#  Kline aggregation
# ════════════════════════════════════════════════════════════════════════════


# Mapping of user-facing interval strings to canonical keys
_INTERVAL_MAP = {
    "1d": "day", "daily": "day", "day": "day",
    "1w": "week", "weekly": "week", "week": "week",
    "1m": "month", "monthly": "month", "month": "month",
    "1y": "year", "yearly": "year", "year": "year",
}


def _get_aggregation_key(interval: str) -> str:
    """Normalise an interval string to one of: day, week, month, year."""
    return _INTERVAL_MAP.get(interval.lower(), "day")


def _get_group_key(date_str: str, agg_key: str) -> str:
    """
    Return a grouping key for *date_str* according to *agg_key*.
    E.g. agg_key='week' → '2025-W07', agg_key='month' → '2025-02'.
    """
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        dt = datetime.strptime(date_str, "%m/%d/%Y")

    if agg_key == "week":
        iso = dt.isocalendar()
        return f"{iso[0]}-W{iso[1]:02d}"
    if agg_key == "month":
        return dt.strftime("%Y-%m")
    if agg_key == "year":
        return dt.strftime("%Y")
    return date_str


def get_stock_kline(
    db: Session, stock_id: int, interval: str, limit: int
) -> Optional[Dict]:
    """
    Fetch and aggregate klines for a stock.

    Supports day / week / month / year intervals. Daily data is returned
    directly; other intervals are aggregated from daily rows.
    """
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None

    symbol = stock.symbol.upper()
    agg_key = _get_aggregation_key(interval)

    # Fetch all daily klines (ascending for aggregation)
    daily_results = (
        db.query(DailyKline)
        .filter(DailyKline.symbol == symbol)
        .order_by(DailyKline.date.asc())
        .all()
    )

    base_response = {
        "stock_id": stock_id,
        "symbol": symbol,
        "interval": interval,
    }

    if not daily_results:
        return {**base_response, "klines": []}

    # ── Daily: just slice and reverse ─────────────────────────────────────
    if agg_key == "day":
        tail = daily_results[-limit:] if len(daily_results) > limit else daily_results
        klines = [
            {
                "date": str(r.date),
                "open": _safe_float(r.open),
                "high": _safe_float(r.high),
                "low": _safe_float(r.low),
                "close": _safe_float(r.close),
                "volume": _safe_float(r.volume),
            }
            for r in reversed(tail)
            if r.open is not None and r.close is not None
        ]
        return {**base_response, "klines": klines}

    # ── Aggregated intervals ──────────────────────────────────────────────
    groups: Dict[str, Dict] = {}
    group_order: List[str] = []

    for r in daily_results:
        if r.open is None or r.high is None or r.low is None or r.close is None:
            continue

        key = _get_group_key(r.date, agg_key)
        r_open = _safe_float(r.open)
        r_high = _safe_float(r.high)
        r_low = _safe_float(r.low)
        r_close = _safe_float(r.close)
        r_vol = _safe_float(r.volume or 0)

        if key not in groups:
            groups[key] = {
                "date": r.date,
                "open": r_open,
                "high": r_high,
                "low": r_low,
                "close": r_close,
                "volume": r_vol,
            }
            group_order.append(key)
        else:
            g = groups[key]
            g["high"] = max(g["high"], r_high)
            g["low"] = min(g["low"], r_low)
            g["close"] = r_close
            g["volume"] += r_vol

    # Take the latest N groups
    klines = []
    for key in reversed(group_order):
        g = groups[key]
        klines.append({
            "date": str(g["date"]),
            "open": g["open"],
            "high": g["high"],
            "low": g["low"],
            "close": g["close"],
            "volume": g["volume"],
        })
        if len(klines) >= limit:
            break

    return {**base_response, "klines": klines}


# ════════════════════════════════════════════════════════════════════════════
#  Stock statistics
# ════════════════════════════════════════════════════════════════════════════


def get_stock_stats(db: Session, stock_id: int) -> Optional[Dict]:
    """Aggregate comprehensive statistics for a stock."""
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None

    symbol = stock.symbol.upper()
    klines = _get_latest_klines(db, symbol, limit=2)
    latest = klines[0] if klines else None
    prev = klines[1] if len(klines) > 1 else None

    # Latest TTM income statement (fallback to any FY)
    income = (
        db.query(IncomeStatement)
        .filter(IncomeStatement.stock_id == stock.id, IncomeStatement.period_type == "TTM")
        .order_by(desc(IncomeStatement.period_ending))
        .first()
    )
    if not income:
        income = (
            db.query(IncomeStatement)
            .filter(IncomeStatement.stock_id == stock.id)
            .order_by(desc(IncomeStatement.period_ending))
            .first()
        )

    # Latest balance sheet
    balance = (
        db.query(BalanceSheet)
        .filter(BalanceSheet.stock_id == stock.id)
        .order_by(desc(BalanceSheet.period_ending))
        .first()
    )

    # Day range & 52-week range strings
    day_range = (
        f"{latest.low:,.2f} - {latest.high:,.2f}"
        if latest and latest.low and latest.high
        else None
    )
    fifty_two_range = (
        f"{latest.week_52_low:,.2f} - {latest.week_52_high:,.2f}"
        if latest and latest.week_52_low and latest.week_52_high
        else None
    )

    return {
        "stock_id": stock_id,
        "symbol": symbol,
        "market_cap": _optional_float(latest.market_cap) if latest else None,
        "revenue_ttm": _optional_float(income.revenue) if income else None,
        "net_income": _optional_float(income.net_income) if income else None,
        "eps": _optional_float(income.eps_basic) if income else None,
        "shares_outstanding": balance.shares_outstanding if balance else None,
        "pe_ratio": latest.pe_ratio if latest else None,
        "forward_pe": latest.forward_pe if latest else None,
        "dividend": latest.dividend_per_share if latest else None,
        "ex_dividend_date": latest.ex_dividend_date if latest else None,
        "volume": latest.volume if latest else None,
        "avg_volume": latest.avg_volume_20d if latest else None,
        "open": latest.open if latest else None,
        "previous_close": prev.close if prev and prev.close else None,
        "day_range": day_range,
        "fifty_two_week_range": fifty_two_range,
        "beta": latest.beta if latest else None,
        "rsi": latest.rsi if latest else None,
        "earnings_date": None,
    }


# ════════════════════════════════════════════════════════════════════════════
#  Stock info
# ════════════════════════════════════════════════════════════════════════════


def get_stock_info(db: Session, stock_id: int) -> Optional[Dict]:
    """Return lightweight profile + technical indicators."""
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None

    symbol = stock.symbol.upper()
    klines = _get_latest_klines(db, symbol, limit=1)
    latest = klines[0] if klines else None

    return {
        "stock_id": stock_id,
        "symbol": symbol,
        "ipo_date": getattr(stock, "founded", None),
        "name": stock.name,
        "fifty_two_week_high": _safe_float(latest.week_52_high) if latest else None,
        "fifty_two_week_low": _safe_float(latest.week_52_low) if latest else None,
        "fifty_day_moving_average": _safe_float(latest.ma_50d) if latest else None,
        "sector": stock.sector,
        "industry": stock.industry,
        "sentiment": getattr(stock, "sentiment", None),
        "sp_score": getattr(stock, "sp_score", None),
    }


# ════════════════════════════════════════════════════════════════════════════
#  Related stocks
# ════════════════════════════════════════════════════════════════════════════


def get_stock_related(db: Session, stock_id: int, limit: int = 10) -> Optional[List[Dict]]:
    """Return stocks in the same sector, with their latest market cap and revenue."""
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None

    related = (
        db.query(Stock)
        .filter(Stock.sector == stock.sector, Stock.id != stock_id)
        .limit(limit)
        .all()
    )

    results = []
    for s in related:
        latest_kline = (
            db.query(DailyKline)
            .filter(DailyKline.symbol == s.symbol)
            .order_by(desc(DailyKline.date))
            .first()
        )
        latest_income = (
            db.query(IncomeStatement)
            .filter(IncomeStatement.stock_id == s.id)
            .order_by(desc(IncomeStatement.period_ending))
            .first()
        )
        results.append({
            "stock_id": s.id,
            "symbol": s.symbol,
            "market_cap": _optional_float(latest_kline.market_cap) if latest_kline else None,
            "revenue_ttm": _optional_float(latest_income.revenue) if latest_income else None,
        })

    return results


# ════════════════════════════════════════════════════════════════════════════
#  Dividends
# ════════════════════════════════════════════════════════════════════════════


def get_stock_dividends(db: Session, stock_id: int) -> Optional[List[Dividend]]:
    """Return all historical dividends for a stock, ordered by ex-dividend date."""
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None
    return (
        db.query(Dividend)
        .filter(Dividend.stock_id == stock_id)
        .order_by(desc(Dividend.ex_dividend_date))
        .all()
    )


# ════════════════════════════════════════════════════════════════════════════
#  Income statement
# ════════════════════════════════════════════════════════════════════════════


def get_stock_by_income_statement(db: Session, stock_id: int) -> Optional[Stock]:
    """Return a Stock with its income_stmts relationship eagerly loadable."""
    return (
        db.query(Stock)
        .join(IncomeStatement, Stock.id == IncomeStatement.stock_id)
        .filter(Stock.id == stock_id)
        .order_by(desc(IncomeStatement.period_ending))
        .first()
    )


def format_income_statement(income_stmt) -> Optional[Dict]:
    """
    Convert an IncomeStatement ORM object into a plain dict
    suitable for the API response.
    """
    if not income_stmt:
        return None

    return {
        "id": income_stmt.id,
        "stock_id": income_stmt.stock_id,
        "period_ending": str(income_stmt.period_ending),
        "period_type": income_stmt.period_type,
        "revenue": _optional_float(income_stmt.revenue),
        "operating_revenue": _optional_float(income_stmt.operating_revenue),
        "other_revenue": _optional_float(income_stmt.other_revenue),
        "revenue_growth_yoy": _optional_float(income_stmt.revenue_growth_yoy),
        "cost_of_revenue": _optional_float(income_stmt.cost_of_revenue),
        "gross_profit": _optional_float(income_stmt.gross_profit),
        "sga_expenses": _optional_float(income_stmt.sga_expenses),
        "operating_income": _optional_float(income_stmt.operating_income),
        "ebitda": _optional_float(income_stmt.ebitda),
        "ebit": _optional_float(income_stmt.ebit),
        "interest_expense": _optional_float(income_stmt.interest_expense),
        "pretax_income": _optional_float(income_stmt.pretax_income),
        "income_tax": _optional_float(income_stmt.income_tax),
        "net_income": _optional_float(income_stmt.net_income),
        "net_income_growth_yoy": _optional_float(income_stmt.net_income_growth_yoy),
        "eps_basic": _optional_float(income_stmt.eps_basic),
        "eps_diluted": _optional_float(income_stmt.eps_diluted),
        "eps_growth_yoy": _optional_float(income_stmt.eps_growth_yoy),
        "dividend_per_share": _optional_float(income_stmt.dividend_per_share),
        "shares_basic": income_stmt.shares_basic,
        "shares_diluted": income_stmt.shares_diluted,
    }


# ════════════════════════════════════════════════════════════════════════════
#  Popular comparisons
# ════════════════════════════════════════════════════════════════════════════


def get_popular_comparisons(db: Session) -> List[Dict]:
    """Return the top-2 stocks per sector ranked by market cap."""
    # Subquery: latest kline date per symbol
    latest_date_sq = (
        db.query(
            DailyKline.symbol,
            func.max(DailyKline.date).label("latest_date"),
        )
        .group_by(DailyKline.symbol)
        .subquery()
    )

    # Subquery: rank stocks within each sector by market cap
    ranked_sq = (
        db.query(
            Stock.id,
            Stock.symbol,
            Stock.sector,
            func.row_number()
            .over(partition_by=Stock.sector, order_by=desc(DailyKline.market_cap))
            .label("rank"),
        )
        .join(DailyKline, Stock.symbol == DailyKline.symbol)
        .join(
            latest_date_sq,
            (DailyKline.symbol == latest_date_sq.c.symbol)
            & (DailyKline.date == latest_date_sq.c.latest_date),
        )
        .subquery()
    )

    rows = (
        db.query(
            ranked_sq.c.id,
            ranked_sq.c.symbol,
            ranked_sq.c.sector,
            ranked_sq.c.rank,
        )
        .filter(ranked_sq.c.rank <= 2)
        .all()
    )

    return [
        {"id": r.id, "symbol": r.symbol, "sector": r.sector, "rank": r.rank}
        for r in rows
    ]


# ════════════════════════════════════════════════════════════════════════════
#  Stock comparison details
# ════════════════════════════════════════════════════════════════════════════


def get_stock_comparison_details(db: Session, stock_id: int) -> Optional[Dict]:
    """Return the full comparison data dict for a single stock."""
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None
    return _build_comparison_data(db, stock)


def _build_comparison_data(db: Session, stock: Stock) -> Dict:
    """
    Assemble comprehensive comparison data for *stock* by pulling from
    klines, income statements, balance sheets, cash flows, and ratios.
    """
    symbol = stock.symbol.upper()

    # Latest 2 klines for price & 1D change
    klines = _get_latest_klines(db, symbol, limit=2)
    latest = klines[0] if klines else None
    prev = klines[1] if len(klines) > 1 else None

    # Latest financials
    income = (
        db.query(IncomeStatement)
        .filter(IncomeStatement.stock_id == stock.id)
        .order_by(desc(IncomeStatement.period_ending))
        .first()
    )
    balance = (
        db.query(BalanceSheet)
        .filter(BalanceSheet.stock_id == stock.id)
        .order_by(desc(BalanceSheet.period_ending))
        .first()
    )
    cash = (
        db.query(CashFlow)
        .filter(CashFlow.stock_id == stock.id)
        .order_by(desc(CashFlow.period_ending))
        .first()
    )
    ratio = (
        db.query(StockRatio)
        .filter(StockRatio.stock_id == stock.id)
        .order_by(desc(StockRatio.period_ending))
        .first()
    )

    # 1-day price change
    price_change_1d = None
    price_change_pct_1d = None
    if latest and prev and latest.close and prev.close:
        price_change_1d = float(latest.close - prev.close)
        price_change_pct_1d = float((latest.close - prev.close) / prev.close * 100)

    return {
        # Basic info
        "symbol": symbol,
        "name": stock.name,
        "sector": stock.sector,
        "industry": stock.industry,
        "exchange": stock.exchange,
        "website": stock.website,
        "country": stock.country,
        "employees": stock.employees,
        "founded": stock.founded,
        "ipo_date": None,

        # Price
        "stock_price": _optional_float(latest.close) if latest else None,
        "price_change_1d": price_change_1d,
        "price_change_percent_1d": price_change_pct_1d,
        "open_price": _optional_float(latest.open) if latest else None,
        "previous_close": _optional_float(prev.close) if prev else None,
        "low_price": _optional_float(latest.low) if latest else None,
        "high_price": _optional_float(latest.high) if latest else None,
        "volume": latest.volume if latest else None,
        "dollar_volume": (
            float(latest.volume * latest.close)
            if latest and latest.volume and latest.close
            else None
        ),
        "stock_price_date": latest.date if latest else None,

        # 52-week
        "fifty_two_week_low": latest.week_52_low if latest else None,
        "fifty_two_week_high": latest.week_52_high if latest else None,

        # Valuation
        "market_cap": _optional_float(latest.market_cap) if latest else None,
        "enterprise_value": _optional_float(latest.enterprise_value) if latest else None,
        "pe_ratio": latest.pe_ratio if latest else None,
        "forward_pe": latest.forward_pe if latest else None,
        "ps_ratio": latest.ps_ratio if latest else None,
        "pb_ratio": latest.pb_ratio if latest else None,
        "peg_ratio": None,
        "ev_sales": _optional_float(ratio.ev_sales) if ratio else None,
        "ev_ebitda": _optional_float(ratio.ev_ebitda) if ratio else None,
        "ev_ebit": _optional_float(ratio.ev_ebit) if ratio else None,
        "ev_fcf": _optional_float(ratio.ev_fcf) if ratio else None,
        "earnings_yield": _optional_float(ratio.earnings_yield) if ratio else None,
        "fcf_yield": _optional_float(ratio.fcf_yield) if ratio else None,

        # Financials
        "revenue": _optional_float(income.revenue) if income else None,
        "gross_profit": _optional_float(income.gross_profit) if income else None,
        "operating_income": _optional_float(income.operating_income) if income else None,
        "net_income": _optional_float(income.net_income) if income else None,
        "ebitda": _optional_float(income.ebitda) if income else None,
        "ebit": _optional_float(income.ebit) if income else None,
        "eps": _optional_float(income.eps_basic) if income else None,
        "revenue_growth": _optional_float(income.revenue_growth_yoy) if income else None,
        "net_income_growth": _optional_float(income.net_income_growth_yoy) if income else None,
        "eps_growth": _optional_float(income.eps_growth_yoy) if income else None,

        # Margins
        "gross_margin": _optional_float(income.gross_margin) if income else None,
        "operating_margin": _optional_float(income.operating_margin) if income else None,
        "profit_margin": _optional_float(income.profit_margin) if income else None,
        "fcf_margin": _optional_float(income.fcf_margin) if income else None,

        # Cash flow
        "operating_cash_flow": _optional_float(cash.operating_cash_flow) if cash else None,
        "investing_cash_flow": _optional_float(cash.investing_cash_flow) if cash else None,
        "financing_cash_flow": _optional_float(cash.financing_cash_flow) if cash else None,
        "net_cash_flow": _optional_float(cash.net_cash_flow) if cash else None,
        "capital_expenditures": _optional_float(cash.capex) if cash else None,
        "free_cash_flow": _optional_float(cash.free_cash_flow) if cash else None,

        # Balance sheet
        "total_cash": _optional_float(balance.cash_and_st_investments) if balance else None,
        "total_debt": _optional_float(balance.total_debt) if balance else None,
        "net_cash_debt": _optional_float(balance.net_cash_debt) if balance else None,
        "total_assets": _optional_float(balance.total_assets) if balance else None,
        "total_liabilities": _optional_float(balance.total_liabilities) if balance else None,
        "shareholders_equity": _optional_float(balance.shareholders_equity) if balance else None,
        "working_capital": _optional_float(balance.working_capital) if balance else None,
        "book_value_per_share": _optional_float(balance.book_value_per_share) if balance else None,
        "shares_outstanding": balance.shares_outstanding if balance else None,

        # Ratios
        "roe": _optional_float(ratio.roe) if ratio else None,
        "roa": _optional_float(ratio.roa) if ratio else None,
        "roic": _optional_float(ratio.roic) if ratio else None,
        "roce": _optional_float(ratio.roce) if ratio else None,
        "current_ratio": _optional_float(ratio.current_ratio) if ratio else None,
        "quick_ratio": _optional_float(ratio.quick_ratio) if ratio else None,
        "debt_equity": _optional_float(ratio.debt_equity) if ratio else None,
        "debt_ebitda": _optional_float(ratio.debt_ebitda) if ratio else None,
        "interest_coverage": _optional_float(ratio.interest_coverage) if ratio else None,
        "altman_z_score": _optional_float(ratio.altman_z_score) if ratio else None,
        "piotroski_f_score": ratio.piotroski_f_score if ratio else None,

        # Technicals
        "rsi": latest.rsi if latest else None,
        "beta": latest.beta if latest else None,
        "ma_20": None,
        "ma_50": latest.ma_50d if latest else None,
        "ma_200": latest.ma_200d if latest else None,

        # Dividends
        "dividend_yield": (
            _optional_float(ratio.dividend_yield)
            if ratio and ratio.dividend_yield
            else (latest.dividend_yield if latest else None)
        ),
        "dividend_per_share": latest.dividend_per_share if latest else None,
        "ex_div_date": latest.ex_dividend_date if latest else None,
    }


# ════════════════════════════════════════════════════════════════════════════
#  Search
# ════════════════════════════════════════════════════════════════════════════


def search_stocks(db: Session, query: str, limit: int = 10) -> List[Stock]:
    """
    Search stocks by symbol or name using case-insensitive LIKE.
    Returns up to *limit* matching Stock rows.
    """
    pattern = f"%{query}%"
    return (
        db.query(Stock)
        .filter(
            (Stock.symbol.ilike(pattern)) | (Stock.name.ilike(pattern))
        )
        .limit(limit)
        .all()
    )


# ════════════════════════════════════════════════════════════════════════════
#  Bulk comparison
# ════════════════════════════════════════════════════════════════════════════


def get_bulk_comparison(
    db: Session,
    symbols: List[str],
    interval: str = "week",
    kline_limit: int = 52,
) -> List[Dict]:
    """
    Fetch klines + stats for multiple symbols in one call.
    Used by the /stocks/compare endpoint.
    """
    results = []
    for sym in symbols:
        stock = db.query(Stock).filter(Stock.symbol.ilike(sym.strip())).first()
        if not stock:
            continue

        kline_data = get_stock_kline(db, stock.id, interval, kline_limit)
        stats_data = get_stock_stats(db, stock.id)

        results.append({
            "stock_id": stock.id,
            "symbol": stock.symbol.upper(),
            "klines": kline_data["klines"] if kline_data else [],
            "stats": stats_data,
        })

    return results