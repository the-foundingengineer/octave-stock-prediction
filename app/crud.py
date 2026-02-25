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
    - get_market_cap_history           : Historical market cap
"""

from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from app.models import (
    BalanceSheet, CashFlow, DailyKline,
    Dividend, IncomeStatement, MarketCapHistory, Stock, StockRatio, StockExecutive,
    User, NewsArticle, Alert, UserActivity, MarketIndex, MacroRate,
)
from app import schemas, models


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


def _resolve_stock(db: Session, stock_id: int) -> Optional[Stock]:
    """Look up a Stock by primary key, or return None."""
    return db.query(Stock).filter(Stock.id == stock_id).first()


def _get_latest_klines(db: Session, stock_id: int, limit: int = 2) -> List[DailyKline]:
    """
    Fetch the latest *limit* daily klines for a stock, ordered newest-first.
    Used by stats, info, and comparison helpers.
    """
    return (
        db.query(DailyKline)
        .filter(DailyKline.stock_id == stock_id)
        .order_by(desc(DailyKline.date))
        .limit(limit)
        .all()
    )


def _get_latest_ratio(db: Session, stock_id: int) -> Optional[StockRatio]:
    """Fetch the latest StockRatio row for a stock."""
    return (
        db.query(StockRatio)
        .filter(StockRatio.stock_id == stock_id)
        .order_by(desc(StockRatio.period_ending))
        .first()
    )


def _get_latest_dividend(db: Session, stock_id: int) -> Optional[Dividend]:
    """Fetch the most recent dividend for a stock."""
    return (
        db.query(Dividend)
        .filter(Dividend.stock_id == stock_id)
        .order_by(desc(Dividend.ex_dividend_date))
        .first()
    )


# ════════════════════════════════════════════════════════════════════════════
#  Basic stock operations
# ════════════════════════════════════════════════════════════════════════════


def get_stock_profile(db: Session, stock_id: int) -> Optional[Stock]:
    """Return a stock with its full profile and executives."""
    return (
        db.query(Stock)
        .filter(Stock.id == stock_id)
        .first()
    )


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
    # Resolve stock_id from symbol
    stock_row = db.query(Stock).filter(Stock.symbol.ilike(stock.symbol)).first()
    if not stock_row:
        raise ValueError(f"Stock with symbol '{stock.symbol}' not found")

    db_stock = DailyKline(
        stock_id=stock_row.id,
        date=stock.date,
        open=stock.open,
        high=stock.high,
        low=stock.low,
        close=stock.close,
        volume=stock.volume,
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

    agg_key = _get_aggregation_key(interval)

    # Fetch all daily klines (ascending for aggregation)
    daily_results = (
        db.query(DailyKline)
        .filter(DailyKline.stock_id == stock_id)
        .order_by(DailyKline.date.asc())
        .all()
    )

    base_response = {
        "stock_id": stock_id,
        "symbol": stock.symbol.upper(),
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

    klines = _get_latest_klines(db, stock_id, limit=2)
    latest = klines[0] if klines else None
    prev = klines[1] if len(klines) > 1 else None

    # Latest financials
    ratio = _get_latest_ratio(db, stock_id)
    dividend = _get_latest_dividend(db, stock_id)

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
        "symbol": stock.symbol.upper(),
        "market_cap": _optional_float(ratio.market_cap) if ratio else None,
        "revenue_ttm": _optional_float(income.revenue) if income else None,
        "net_income": _optional_float(income.net_income) if income else None,
        "eps": _optional_float(income.eps_basic) if income else None,
        "shares_outstanding": balance.shares_outstanding if balance else None,
        "pe_ratio": _optional_float(ratio.pe_ratio) if ratio else None,
        "forward_pe": None,  # Not stored independently; derive from ratio if needed
        "dividend": _optional_float(dividend.amount) if dividend else None,
        "ex_dividend_date": dividend.ex_dividend_date if dividend else None,
        "volume": latest.volume if latest else None,
        "avg_volume": latest.avg_volume_20d if latest else None,
        "open": latest.open if latest else None,
        "previous_close": prev.close if prev and prev.close else None,
        "day_range": day_range,
        "fifty_two_week_range": fifty_two_range,
        "beta": latest.beta if latest else None,
        "rsi": latest.rsi if latest else None,
        "earnings_date": None,
        "payout_ratio": _optional_float(ratio.payout_ratio) if ratio else None,
        "dividend_growth": None,
        "payout_frequency": dividend.frequency if dividend else None,
        "revenue_growth": _optional_float(income.revenue_growth_yoy) if income else None,
        "revenue_per_employee": None,
    }


# ════════════════════════════════════════════════════════════════════════════
#  Stock info
# ════════════════════════════════════════════════════════════════════════════


def get_stock_info(db: Session, stock_id: int) -> Optional[Dict]:
    """Return lightweight profile + technical indicators."""
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None

    klines = _get_latest_klines(db, stock_id, limit=1)
    latest = klines[0] if klines else None

    return {
        "stock_id": stock_id,
        "symbol": stock.symbol.upper(),
        "ipo_date": str(stock.ipo_date) if stock.ipo_date else None,
        "name": stock.name,
        "fifty_two_week_high": _safe_float(latest.week_52_high) if latest else None,
        "fifty_two_week_low": _safe_float(latest.week_52_low) if latest else None,
        "fifty_day_moving_average": _safe_float(latest.ma_50d) if latest else None,
        "sector": stock.sector,
        "industry": stock.industry,
        "sentiment": stock.sentiment,
        "sp_score": stock.sp_score,
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
        latest_ratio = _get_latest_ratio(db, s.id)
        latest_income = (
            db.query(IncomeStatement)
            .filter(IncomeStatement.stock_id == s.id)
            .order_by(desc(IncomeStatement.period_ending))
            .first()
        )
        results.append({
            "stock_id": s.id,
            "symbol": s.symbol,
            "market_cap": _optional_float(latest_ratio.market_cap) if latest_ratio else None,
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

    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None
    return (
        db.query(IncomeStatement)
        .filter(IncomeStatement.stock_id == stock_id)
        .order_by(desc(IncomeStatement.period_ending))
        .all()
        # db.query(Stock)
        # .join(IncomeStatement, Stock.id == IncomeStatement.stock_id)
        # .filter(Stock.id == stock_id)
        # .order_by(desc(IncomeStatement.period_ending))
        # .first()
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
    # Subquery: latest ratio per stock (for market_cap)
    latest_ratio_sq = (
        db.query(
            StockRatio.stock_id,
            func.max(StockRatio.period_ending).label("latest_period"),
        )
        .group_by(StockRatio.stock_id)
        .subquery()
    )

    # Subquery: rank stocks within each sector by market cap
    ranked_sq = (
        db.query(
            Stock.id,
            Stock.symbol,
            Stock.sector,
            func.row_number()
            .over(partition_by=Stock.sector, order_by=desc(StockRatio.market_cap))
            .label("rank"),
        )
        .join(StockRatio, Stock.id == StockRatio.stock_id)
        .join(
            latest_ratio_sq,
            (StockRatio.stock_id == latest_ratio_sq.c.stock_id)
            & (StockRatio.period_ending == latest_ratio_sq.c.latest_period),
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
    klines = _get_latest_klines(db, stock.id, limit=2)
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
    ratio = _get_latest_ratio(db, stock.id)
    dividend = _get_latest_dividend(db, stock.id)

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
        "stock_exchange": stock.stock_exchange,
        "website": stock.website,
        "country": stock.country,
        "employees": stock.employees,
        "founded": stock.founded,
        "ipo_date": stock.ipo_date,

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

        # Valuation (from stock_ratios)
        "market_cap": _optional_float(ratio.market_cap) if ratio else None,
        "enterprise_value": _optional_float(ratio.enterprise_value) if ratio else None,
        "pe_ratio": _optional_float(ratio.pe_ratio) if ratio else None,
        "forward_pe": None,
        "ps_ratio": _optional_float(ratio.ps_ratio) if ratio else None,
        "pb_ratio": _optional_float(ratio.pb_ratio) if ratio else None,
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

        # Technicals (still on DailyKline)
        "rsi": latest.rsi if latest else None,
        "beta": latest.beta if latest else None,
        "ma_20": None,
        "ma_50": latest.ma_50d if latest else None,
        "ma_200": latest.ma_200d if latest else None,

        # Dividends (from dividends + stock_ratios)
        "dividend_yield": (
            _optional_float(ratio.dividend_yield)
            if ratio and ratio.dividend_yield
            else None
        ),
        "dividend_per_share": _optional_float(dividend.amount) if dividend else None,
        "ex_div_date": dividend.ex_dividend_date if dividend else None,
        "payout_ratio": _optional_float(ratio.payout_ratio) if ratio else None,
        "dividend_growth": None,
        "payout_frequency": dividend.frequency if dividend else None,
        "revenue_ttm": _optional_float(income.revenue) if income else None,
        "revenue_growth": _optional_float(income.revenue_growth_yoy) if income else None,
        "revenue_per_employee": None,
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


# ════════════════════════════════════════════════════════════════════════════
#  Market cap history
# ════════════════════════════════════════════════════════════════════════════


def get_market_cap_history(
    db: Session, stock_id: int, limit: int = 500
) -> Optional[Dict]:
    """Return historical market cap data for a stock, newest first."""
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None

    rows = (
        db.query(MarketCapHistory)
        .filter(MarketCapHistory.stock_id == stock_id)
        .order_by(desc(MarketCapHistory.date))
        .limit(limit)
        .all()
    )

    return {
        "stock_id": stock_id,
        "symbol": stock.symbol.upper(),
        "history": [
            {
                "id": r.id,
                "stock_id": r.stock_id,
                "date": r.date,
                "market_cap": float(r.market_cap) if r.market_cap else None,
                "frequency": r.frequency,
            }
            for r in rows
        ],
    }


def get_metric_comparison(
    db: Session,
    symbols: List[str],
    metric: str,
    limit: int = 20,
) -> List[Dict]:
    """
    Fetch historical data for a specific metric across multiple stocks.
    Supported metrics: revenue, market_cap, net_income, eps, free_cash_flow,
    pe_ratio, pb_ratio, ps_ratio.
    """
    results = []
    metric = metric.lower()

    for sym in symbols:
        stock = db.query(Stock).filter(Stock.symbol.ilike(sym.strip())).first()
        if not stock:
            continue

        data_points = []

        if metric == "market_cap":
            # Primary source: MarketCapHistory
            rows = (
                db.query(MarketCapHistory)
                .filter(MarketCapHistory.stock_id == stock.id)
                .order_by(desc(MarketCapHistory.date))
                .limit(limit)
                .all()
            )
            data_points = [
                {"date": str(r.date), "value": float(r.market_cap) if r.market_cap else None}
                for r in rows
            ]
        elif metric in ["revenue", "net_income", "eps", "free_cash_flow"]:
            # Primary source: IncomeStatement (period_type='FY' or 'TTM')
            field_map = {
                "revenue": IncomeStatement.revenue,
                "net_income": IncomeStatement.net_income,
                "eps": IncomeStatement.eps_basic,
                "free_cash_flow": IncomeStatement.free_cash_flow,
            }
            rows = (
                db.query(IncomeStatement.period_ending, field_map[metric])
                .filter(IncomeStatement.stock_id == stock.id)
                .order_by(desc(IncomeStatement.period_ending))
                .limit(limit)
                .all()
            )
            data_points = [
                {"date": str(r[0]), "value": float(r[1]) if r[1] else None}
                for r in rows
            ]
        elif metric in ["pe_ratio", "pb_ratio", "ps_ratio"]:
            # Primary source: StockRatio
            field_map = {
                "pe_ratio": StockRatio.pe_ratio,
                "pb_ratio": StockRatio.pb_ratio,
                "ps_ratio": StockRatio.ps_ratio,
            }
            rows = (
                db.query(StockRatio.period_ending, field_map[metric])
                .filter(StockRatio.stock_id == stock.id)
                .order_by(desc(StockRatio.period_ending))
                .limit(limit)
                .all()
            )
            data_points = [
                {"date": str(r[0]), "value": float(r[1]) if r[1] else None}
                for r in rows
            ]

        # Ensure data is chronological (oldest first for charts)
        data_points.reverse()

        results.append({
            "stock_id": stock.id,
            "symbol": stock.symbol.upper(),
            "metric": metric,
            "data": data_points,
        })

    return results


def get_stocks_dashboard(db: Session, page: int = 1, limit: int = 20) -> Dict:
    """
    Fetch a list of stocks with their latest metrics, price performance,
    and a 7-day sparkline.
    """
    offset = (page - 1) * limit
    total = db.query(Stock).count()
    stocks = db.query(Stock).order_by(Stock.symbol).offset(offset).limit(limit).all()

    items = []
    for s in stocks:
        # Latest 8 klines (today + last 7 days for sparkline and change calc)
        klines = (
            db.query(DailyKline)
            .filter(DailyKline.stock_id == s.id)
            .order_by(desc(DailyKline.date))
            .limit(8)
            .all()
        )

        if not klines:
            items.append({
                "symbol": s.symbol.upper(),
                "name": s.name,
                "price": None,
                "change_1h": None,
                "change_24h": None,
                "change_7d": None,
                "market_cap": None,
                "volume_24h": None,
                "sparkline_7d": [],
            })
            continue

        latest = klines[0]
        prev_24h = klines[1] if len(klines) > 1 else None
        prev_7d = klines[-1] if len(klines) >= 8 else None

        # Price performance
        change_24h = None
        if latest.close and prev_24h and prev_24h.close:
            change_24h = ((latest.close - prev_24h.close) / prev_24h.close) * 100

        change_7d = None
        if latest.close and prev_7d and prev_7d.close:
            change_7d = ((latest.close - prev_7d.close) / prev_7d.close) * 100

        # Latest market cap from ratios
        ratio = (
            db.query(StockRatio)
            .filter(StockRatio.stock_id == s.id)
            .order_by(desc(StockRatio.period_ending))
            .first()
        )

        # Sparkline (7 days, oldest first)
        sparkline_data = [
            {"date": str(k.date), "value": float(k.close) if k.close else None}
            for k in reversed(klines[:7])
        ]

        items.append({
            "id": s.id,
            "symbol": s.symbol.upper(),
            "name": s.name,
            "price": float(latest.close) if latest.close else None,
            "change_1h": None,  # No intraday data available
            "change_24h": change_24h,
            "change_7d": change_7d,
            "market_cap": float(ratio.market_cap) if ratio and ratio.market_cap else None,
            "volume_24h": float(latest.volume) if latest.volume else None,
            "sparkline_7d": sparkline_data,
        })

    return {
        "stocks": items,
        "total": total,
        "page": page,
        "limit": limit
    }


# ── Fear & Greed Index ──────────────────────────────────────────────────────


def _normalize(value: Optional[float], min_val: float, max_val: float, invert: bool = False) -> Optional[float]:
    """Normalize a value to a 0-100 scale using historical min/max."""
    if value is None or max_val == min_val:
        return None
    score = 100 * (value - min_val) / (max_val - min_val)
    score = max(0.0, min(100.0, score))  # clamp
    return 100.0 - score if invert else score


def get_fear_greed_index(db: Session) -> Dict:
    """
    Compute the Nigerian Fear & Greed Index (0-100) from 5 indicators:
    1. Market Momentum  : ASI vs 125-day moving average
    2. Market Breadth   : Advancers vs Decliners (from daily_klines)
    3. Volume Strength  : Volume in advancing stocks / total volume
    4. Volatility       : 30-day StdDev of ASI daily returns (inverted)
    5. Safe Haven Demand: Latest T-Bill yield vs 30-day ASI return (inverted)
    """
    import statistics
    from datetime import date

    indicators: Dict[str, Optional[float]] = {}
    scores: Dict[str, Optional[float]] = {}

    # ── 1. Market Momentum (ASI vs 125d MA) ──────────────────────────────────
    asi_rows = (
        db.query(MarketIndex)
        .filter(MarketIndex.symbol == "NGSEINDEX")
        .order_by(desc(MarketIndex.date))
        .limit(200)
        .all()
    )

    if len(asi_rows) >= 2:
        latest_asi = float(asi_rows[0].price) if asi_rows[0].price else None
        prices_125 = [float(r.price) for r in asi_rows[:125] if r.price]
        ma_125 = statistics.mean(prices_125) if len(prices_125) >= 50 else None

        if latest_asi and ma_125:
            momentum = (latest_asi - ma_125) / ma_125 * 100
            indicators["momentum"] = momentum
            scores["momentum"] = _normalize(momentum, -20.0, 20.0)
        else:
            indicators["momentum"] = None
            scores["momentum"] = None
    else:
        indicators["momentum"] = None
        scores["momentum"] = None

    # ── 2. Market Breadth & 3. Volume Strength ────────────────────────────────
    # Get the two most recent distinct dates from daily_klines
    recent_dates = (
        db.query(DailyKline.date)
        .distinct()
        .order_by(desc(DailyKline.date))
        .limit(2)
        .all()
    )

    if len(recent_dates) >= 2:
        today_date = recent_dates[0][0]
        prev_date = recent_dates[1][0]

        today_klines = (
            db.query(DailyKline)
            .filter(DailyKline.date == today_date)
            .all()
        )
        prev_klines = (
            db.query(DailyKline)
            .filter(DailyKline.date == prev_date)
            .all()
        )

        # Build a map of {stock_id: prev_close}
        prev_map = {k.stock_id: k.close for k in prev_klines if k.close}

        advancers_vol = 0.0
        decliners_vol = 0.0
        total_stocks = 0
        advancing_count = 0

        for k in today_klines:
            prev_close = prev_map.get(k.stock_id)
            if not prev_close or not k.close:
                continue
            total_stocks += 1
            vol = float(k.volume or 0)
            if k.close > prev_close:
                advancing_count += 1
                advancers_vol += vol
            else:
                decliners_vol += vol

        if total_stocks > 0:
            breadth_ratio = advancing_count / total_stocks
            indicators["breadth"] = breadth_ratio
            scores["breadth"] = _normalize(breadth_ratio, 0.0, 1.0)
        else:
            indicators["breadth"] = None
            scores["breadth"] = None

        total_vol = advancers_vol + decliners_vol
        if total_vol > 0:
            vol_ratio = advancers_vol / total_vol
            indicators["volume_strength"] = vol_ratio
            scores["volume_strength"] = _normalize(vol_ratio, 0.0, 1.0)
        else:
            indicators["volume_strength"] = None
            scores["volume_strength"] = None
    else:
        indicators["breadth"] = None
        scores["breadth"] = None
        indicators["volume_strength"] = None
        scores["volume_strength"] = None

    # ── 4. Volatility (20d StdDev of ASI Returns, inverted) ──────────────────
    if len(asi_rows) >= 21:
        recent_prices = [float(r.price) for r in asi_rows[:21] if r.price]
        if len(recent_prices) >= 10:
            daily_returns = [
                (recent_prices[i] - recent_prices[i + 1]) / recent_prices[i + 1]
                for i in range(len(recent_prices) - 1)
            ]
            if len(daily_returns) >= 2:
                volatility = statistics.stdev(daily_returns) * 100
                indicators["volatility"] = volatility
                scores["volatility"] = _normalize(volatility, 0.0, 5.0, invert=True)
            else:
                indicators["volatility"] = None
                scores["volatility"] = None
        else:
            indicators["volatility"] = None
            scores["volatility"] = None
    else:
        indicators["volatility"] = None
        scores["volatility"] = None

    # ── 5. Safe Haven Demand (Bond yield vs 20d ASI return, inverted) ────────
    latest_bond = (
        db.query(MacroRate)
        .filter(MacroRate.symbol.in_(["NGN_3M_TBILL", "NGN_10Y_BOND"]))
        .order_by(desc(MacroRate.date))
        .first()
    )

    if latest_bond and len(asi_rows) >= 20:
        bond_yield = float(latest_bond.value) if latest_bond.value else None
        prices_20 = [float(r.price) for r in asi_rows[:21] if r.price]
        if len(prices_20) >= 2:
            asi_20d_return = (prices_20[0] - prices_20[-1]) / prices_20[-1] * 100
            if bond_yield is not None:
                # High yield vs low stock return = Fear
                # For 10Y, we might need different normalization, but 0-100 scale handles it.
                safe_haven_score = asi_20d_return - bond_yield / 12
                indicators["safe_haven"] = safe_haven_score
                scores["safe_haven"] = _normalize(safe_haven_score, -30.0, 30.0)
            else:
                indicators["safe_haven"] = None
                scores["safe_haven"] = None
        else:
            indicators["safe_haven"] = None
            scores["safe_haven"] = None
    else:
        indicators["safe_haven"] = None
        scores["safe_haven"] = None

    # ── Aggregate ─────────────────────────────────────────────────────────────
    valid_scores = [v for v in scores.values() if v is not None]
    final_score = statistics.mean(valid_scores) if valid_scores else None

    def classify(score: Optional[float]) -> str:
        if score is None:
            return "Insufficient Data"
        if score <= 20:
            return "Extreme Fear"
        if score <= 40:
            return "Fear"
        if score <= 60:
            return "Neutral"
        if score <= 80:
            return "Greed"
        return "Extreme Greed"

    return {
        "score": round(final_score, 1) if final_score is not None else None,
        "classification": classify(final_score),
        "as_of": str(date.today()),
        "components": {
            "market_momentum": {
                "raw": round(indicators["momentum"], 4) if indicators["momentum"] is not None else None,
                "score": round(scores["momentum"], 1) if scores["momentum"] is not None else None,
                "label": "ASI vs 125-day MA",
            },
            "market_breadth": {
                "raw": round(indicators["breadth"], 4) if indicators["breadth"] is not None else None,
                "score": round(scores["breadth"], 1) if scores["breadth"] is not None else None,
                "label": "Advancers / Total Stocks",
            },
            "volume_strength": {
                "raw": round(indicators["volume_strength"], 4) if indicators["volume_strength"] is not None else None,
                "score": round(scores["volume_strength"], 1) if scores["volume_strength"] is not None else None,
                "label": "Advancer Volume / Total Volume",
            },
            "volatility": {
                "raw": round(indicators["volatility"], 4) if indicators["volatility"] is not None else None,
                "score": round(scores["volatility"], 1) if scores["volatility"] is not None else None,
                "label": "20-day ASI Return StdDev (inverted)",
            },
            "safe_haven_demand": {
                "raw": round(indicators["safe_haven"], 4) if indicators["safe_haven"] is not None else None,
                "score": round(scores["safe_haven"], 1) if scores["safe_haven"] is not None else None,
                "label": f"ASI 20d Return vs {latest_bond.name if latest_bond else 'Risk-Free Rate'}",
            },
        },
        "data_availability": {
            "asi_rows": len(asi_rows),
            "macro_indicator": latest_bond.symbol if latest_bond else None,
        },
    }


# ── Users & Auth ──────────────────────────────────────────────────────────────


from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

def get_user_by_email(db: Session, email: str) -> Optional[Stock]: # models.User
    # Note: Stock used as placeholder for models.User if not imported yet, but I'll use strings or ensure imports
    return db.query(models.User).filter(models.User.email == email).first()

def create_user(db: Session, user: schemas.UserCreate) -> models.User:
    hashed_password = pwd_context.hash(user.password)
    db_user = models.User(
        email=user.email,
        hashed_password=hashed_password,
        full_name=user.full_name
    )
    db.add(db_user)
    db.commit()
    db.refresh(db_user)
    return db_user


# ── News ────────────────────────────────────────────────────────────────────


def get_news_articles(db: Session, stock_id: int) -> List[NewsArticle]:
    return db.query(NewsArticle).filter(NewsArticle.stock_id == stock_id).order_by(NewsArticle.published_at.desc()).limit(5).all()




# ── Alerts ──────────────────────────────────────────────────────────────────


def create_alert(db: Session, alert: schemas.AlertCreate, user_id: int) -> models.Alert:
    db_alert = models.Alert(
        user_id=user_id,
        stock_id=alert.stock_id,
        keyword=alert.keyword
    )
    db.add(db_alert)
    db.commit()
    db.refresh(db_alert)
    return db_alert

def get_user_alerts(db: Session, user_id: int) -> List[models.Alert]:
    return db.query(models.Alert).filter(models.Alert.user_id == user_id).all()


# ── Activity ────────────────────────────────────────────────────────────────


def log_activity(db: Session, activity: schemas.UserActivityCreate, user_id: int) -> models.UserActivity:
    db_activity = models.UserActivity(
        user_id=user_id,
        article_id=activity.article_id,
        activity_type=activity.activity_type
    )
    # Simple personalization: boost article rank on click
    article = db.query(models.NewsArticle).filter(models.NewsArticle.id == activity.article_id).first()
    if article:
        # article.rank_score += 10.0 # removed from model
        pass
    db.add(db_activity)
    db.commit()
    db.refresh(db_activity)
    return db_activity


# ════════════════════════════════════════════════════════════════════════════
#  News articles
# ════════════════════════════════════════════════════════════════════════════


def get_news_articles(db: Session, stock_id: int, limit: int = 20) -> List[NewsArticle]:
    """Return news articles for a specific stock, ordered by most recent."""
    return (
        db.query(NewsArticle)
        .filter(NewsArticle.stock_id == stock_id)
        .order_by(desc(NewsArticle.published_at))
        .limit(limit)
        .all()
    )


def get_latest_news(db: Session, limit: int = 50) -> List[NewsArticle]:
    """Return the most recent news articles across all stocks."""
    return db.query(NewsArticle).order_by(desc(NewsArticle.published_at)).limit(limit).all()

