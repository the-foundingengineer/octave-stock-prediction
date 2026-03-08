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
    - get_market_suggestions           : Trending topics and AI questions
"""

from datetime import datetime
from typing import Dict, List, Optional
import traceback

from sqlalchemy import desc, func
from sqlalchemy.orm import Session

from app.models import (
    BalanceSheet, CashFlow, DailyKline,
    Dividend, IncomeStatement, MarketCapHistory, Stock, StockRatio, StockExecutive,
    User, NewsArticle, Alert, UserActivity, MarketIndex, MacroRate,
)
from app import schemas, models

import pandas as pd
import ta
import numpy as np


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
        "stock_id": stock.id,
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
        # Comprehensive stats / comparison info (80+ fields)
        stats_data = get_stock_comparison_details(db, stock.id)

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
            "high_24h": float(latest.high) if latest.high else None,
            "low_24h": float(latest.low) if latest.low else None,
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


def get_equities_screener(
    db: Session,
    view: str = "overview",
    sector: Optional[str] = None,
    sort_by: Optional[str] = None,
    order: str = "desc",
    page: int = 1,
    limit: int = 50
) -> Dict:
    """
    Unified equities screener for Overview, Technical, Performance, and Fundamental views.
    """
    offset = (page - 1) * limit
    query = db.query(Stock)

    if sector:
        query = query.filter(Stock.sector == sector)

    total = query.count()
    stocks = query.offset(offset).limit(limit).all()

    items = []
    for s in stocks:
        # Latest data points
        klines = _get_latest_klines(db, s.id, limit=30)
        latest = klines[0] if klines else None
        prev = klines[1] if len(klines) > 1 else None
        avg_ratio = _get_latest_ratio(db, s.id)
        latest_income = db.query(IncomeStatement).filter(IncomeStatement.stock_id == s.id).order_by(desc(IncomeStatement.period_ending)).first()

        item = {"id": s.id, "symbol": s.symbol.upper(), "name": s.name}

        change = float(latest.close - prev.close) if latest and prev and latest.close and prev.close else 0.0
        change_pct = (change / float(prev.close)) * 100 if prev and prev.close else 0.0

        if view == "overview":
            item.update({
                "price": float(latest.close) if latest and latest.close else None,
                "high_24h": float(latest.high) if latest and latest.high else None,
                "low_24h": float(latest.low) if latest and latest.low else None,
                "change_abs": change,
                "change_24h": change_pct,
                "volume_24h": latest.volume if latest else None
            })

        elif view == "technical":
            if klines:
                import pandas as pd
                df = pd.DataFrame([{"close": k.close, "high": k.high, "low": k.low, "volume": k.volume} for k in reversed(klines)])
                tech = _calculate_technical_indicators(df)
                summary = tech.get("summary", "Neutral")
                item.update({
                    "technical_hourly": summary,  # Simple proxy for now
                    "technical_daily": summary,
                    "technical_weekly": summary,
                    "technical_monthly": summary
                })
            else:
                item.update({
                    "technical_hourly": "Neutral",
                    "technical_daily": "Neutral",
                    "technical_weekly": "Neutral",
                    "technical_monthly": "Neutral"
                })

        elif view == "performance":
            item.update({
                "daily_return": change_pct,
                "weekly_return": ((float(latest.close) - float(klines[4].close)) / float(klines[4].close) * 100) if latest and len(klines) > 4 and klines[4].close else 0.0,
                "monthly_return": ((float(latest.close) - float(klines[-1].close)) / float(klines[-1].close) * 100) if latest and len(klines) > 20 and klines[-1].close else 0.0,
                "ytd_return": 0.0,
                "yearly_return": 0.0,
                "three_year_return": 0.0
            })

        elif view == "fundamental":
            item.update({
                "avg_volume_3m": latest.volume if latest else 0,
                "market_cap": float(avg_ratio.market_cap) if avg_ratio and avg_ratio.market_cap else 0,
                "revenue": float(latest_income.revenue) if latest_income and latest_income.revenue else 0,
                "pe_ratio": float(avg_ratio.pe_ratio) if avg_ratio and avg_ratio.pe_ratio else 0,
                "beta": float(avg_ratio.beta) if avg_ratio and avg_ratio.beta else 0,
                "yield": float(avg_ratio.dividend_yield) if avg_ratio and avg_ratio.dividend_yield else 0,
            })

        elif view == "charts":
            item.update({
                "price": float(latest.close) if latest and latest.close else 0.0,
                "change_24h": change_pct,
                "sparkline_7d": [float(k.close) for k in reversed(klines)] if klines else []
            })

        items.append(item)

    if sort_by and items and sort_by in items[0]:
        items.sort(key=lambda x: (x.get(sort_by) is None, x.get(sort_by)), reverse=(order == "desc"))

    return {
        "items": items,
        "total": total,
        "page": page,
        "limit": limit
    }


def get_stock_detailed_analysis(db: Session, stock_id: int) -> Optional[Dict]:
    """
    Comprehensive stock analysis including valuation, health, and multi-timeframe technicals.
    """
    stock = _resolve_stock(db, stock_id)
    if not stock:
        return None

    klines_daily = _get_latest_klines(db, stock_id, limit=250)
    import pandas as pd
    df_daily = pd.DataFrame([{"close": k.close, "high": k.high, "low": k.low, "volume": k.volume} for k in reversed(klines_daily)])
    
    valuation = _calculate_fair_value(db, stock)
    health = _calculate_health_score(db, stock)
    tech_daily = _calculate_technical_indicators(df_daily)
    
    peers = db.query(Stock).filter(Stock.sector == stock.sector, Stock.id != stock_id).limit(5).all()
    peer_list = []
    for p in peers:
        p_ratio = _get_latest_ratio(db, p.id)
        peer_list.append({
            "id": p.id,
            "symbol": p.symbol.upper(),
            "name": p.name,
            "price": float(p_ratio.last_close_price) if p_ratio and p_ratio.last_close_price else None,
            "market_cap": float(p_ratio.market_cap) if p_ratio and p_ratio.market_cap else None,
            "pe_ratio": float(p_ratio.pe_ratio) if p_ratio and p_ratio.pe_ratio else None,
        })

    news = get_news_articles(db, stock_id, limit=10)
    dividends_objs = db.query(Dividend).filter(Dividend.stock_id == stock_id).order_by(desc(Dividend.ex_dividend_date)).limit(10).all()
    
    # Serialize dividends
    dividends = []
    for d in dividends_objs:
        dividends.append({
            "id": d.id,
            "stock_id": d.stock_id,
            "ex_dividend_date": d.ex_dividend_date,
            "record_date": d.record_date,
            "pay_date": d.pay_date,
            "amount": _safe_float(d.amount),
            "currency": d.currency,
            "frequency": d.frequency
        })

    # Comprehensive stats / comparison info (80+ fields)
    stats = get_stock_comparison_details(db, stock_id)
    
    # 1D Change
    klines_2 = _get_latest_klines(db, stock_id, limit=2)
    latest = klines_2[0] if klines_2 else None
    prev = klines_2[1] if len(klines_2) > 1 else None
    
    change = 0.0
    change_percent = 0.0
    if latest and prev and prev.close:
        change = float(latest.close - prev.close)
        change_percent = float((latest.close - prev.close) / prev.close * 100)

    result = {
        "stock_id": stock.id,
        "symbol": stock.symbol.upper(),
        "name": stock.name,
        "sector": stock.sector,
        "industry": stock.industry,
        
        # Profile fields
        "description": stock.description,
        "website": stock.website,
        "headquarters": stock.headquarters,
        "employees": stock.employees,
        "founded": stock.founded,
        "exchange": stock.stock_exchange,
        "currency": stock.currency,
        "executives": [
            {
                "id": e.id,
                "name": e.name,
                "title": e.title,
                "age": e.age,
                "since": e.since
            } for e in stock.executives
        ],

        # Quote
        "price": float(latest.close) if latest and latest.close else None,
        "change": change,
        "change_percent": change_percent,
        "last_updated": latest.date if latest else None,

        "valuation": valuation,
        "health": health,
        "technical_analysis": [
            {
                "timeframe": "1D",
                "summary": tech_daily.get("summary", "Neutral"),
                "indicators": tech_daily.get("indicators", []),
                "moving_averages": tech_daily.get("moving_averages", [])
            }
        ],
        "latest_dividends": dividends,
        "peers": peer_list,
        "news": news,
        "stats": stats
    }
    return result




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


# ════════════════════════════════════════════════════════════════════════════
#  Market Indices
# ════════════════════════════════════════════════════════════════════════════


def get_market_indices(db: Session):
    """
    Calculate SP10 and SP30 indices dynamically based on the top stocks by market cap.
    Returns: {
        "sp10": { "current_price", "change_24h", "low_52w", "chart_data": [{"date", "price"}] },
        "sp30": { ... }
    }
    """
    from datetime import datetime, timedelta
    from sqlalchemy import desc
    from app.models import StockMetric, DailyKline
    
    # 1. Get top 30 stocks by market cap
    # We use StockMetric to find the latest available market_cap per stock.
    # Postgres specific distinct ON is tricky across dialects, so we fetch all 
    # and filter in memory, or assume the most recent metric per stock is high.
    # For simplicity, let's grab the absolute highest market_cap rows in the last year
    # and uniquely identify up to 30 stocks.
    one_year_ago = datetime.utcnow() - timedelta(days=365)
    
    metrics = (
        db.query(StockMetric.stock_id, StockMetric.market_cap)
        .filter(StockMetric.market_cap != None)
        .order_by(desc(StockMetric.market_cap))
        .limit(100) # Fetch more to account for duplicates
        .all()
    )
    
    seen_stocks = set()
    top_stocks = []
    for m in metrics:
        if m.stock_id not in seen_stocks:
            seen_stocks.add(m.stock_id)
            top_stocks.append(m.stock_id)
            if len(top_stocks) >= 30:
                break
                
    sp10_ids = top_stocks[:10]
    sp30_ids = top_stocks[:30]
    
    if not sp10_ids:
        return {"sp10": None, "sp30": None}
        
    # 2. Fetch 1-yr klines for these 30 stocks
    # Ideally, we want one price per date.
    klines = (
        db.query(DailyKline.stock_id, DailyKline.date, DailyKline.close)
        .filter(DailyKline.stock_id.in_(sp30_ids))
        .filter(DailyKline.close != None)
        # Assuming date string format 'YYYY-MM-DD' validates reasonably well for >=
        .filter(DailyKline.date >= one_year_ago.strftime("%Y-%m-%d"))
        .order_by(DailyKline.date)
        .all()
    )
    
    # 3. Aggregate into daily index values
    from collections import defaultdict
    
    # map: date -> { stock_id: close_price }
    date_prices = defaultdict(dict)
    for k in klines:
        date_prices[k.date][k.stock_id] = float(k.close)
        
    # Sort dates
    sorted_dates = sorted(date_prices.keys())
    
    def calculate_index(target_ids, base_value=100.0):
        chart_data = []
        low_52w = float('inf')
        
        # Determine initial sum to act as our divisor reference
        initial_sum = 0
        if sorted_dates:
            first_date = sorted_dates[0]
            # Sum up whatever subset of target_ids existed on day 1
            for sid in target_ids:
                initial_sum += date_prices[first_date].get(sid, 0.0)
                
        divisor = initial_sum / base_value if initial_sum > 0 else 1.0
        
        for d in sorted_dates:
            daily_sum = 0.0
            valid_count = 0
            for sid in target_ids:
                price = date_prices[d].get(sid)
                if price is not None:
                    daily_sum += price
                    valid_count += 1
            
            # Simple divisor logic (price-weighted)
            index_val = daily_sum / divisor if divisor > 0 else 0
            # If no data for this day, skip or carry forward
            if valid_count > 0:
                chart_data.append({"date": d, "price": round(index_val, 2)})
                if index_val < low_52w:
                    low_52w = index_val
                    
        # Extract stats
        if not chart_data:
            return None
            
        current_price = chart_data[-1]["price"]
        prev_price = chart_data[-2]["price"] if len(chart_data) > 1 else current_price
        
        change_24h = ((current_price - prev_price) / prev_price * 100) if prev_price > 0 else 0.0
        
        return {
            "current_price": current_price,
            "change_24h": round(change_24h, 2),
            "low_52w": round(low_52w, 2),
            "chart_data": chart_data
        }

    return {
        "sp10": calculate_index(sp10_ids, base_value=100.0),
        "sp30": calculate_index(sp30_ids, base_value=100.0)
    }


# ════════════════════════════════════════════════════════════════════════════
#  Top Gainers and Top Losers
# ════════════════════════════════════════════════════════════════════════════

def _get_timeframe_delta(timeframe: str):
    """Convert timeframe string to timedelta and label."""
    from datetime import timedelta
    tf = timeframe.lower()
    if tf == "1h": return timedelta(hours=1), "1h"
    if tf == "1d": return timedelta(days=1), "24h"
    if tf == "1w": return timedelta(weeks=1), "7d"
    if tf == "1m": return timedelta(days=30), "30d"
    if tf == "1y": return timedelta(days=365), "1y"
    # Fallback to 1d
    return timedelta(days=1), "24h"

def get_top_gainers(db: Session, limit: int = 10, timeframe: str = "1d") -> List[Stock]:
    """Get top performing stocks by price change in a given timeframe."""
    return _get_top_moved_stocks(db, limit, timeframe, sort_desc=True)

def get_top_losers(db: Session, limit: int = 10, timeframe: str = "1d") -> List[Stock]:
    """Get worst performing stocks by price change in a given timeframe."""
    return _get_top_moved_stocks(db, limit, timeframe, sort_desc=False)

def _get_top_moved_stocks(db: Session, limit: int, timeframe: str, sort_desc: bool) -> List[Stock]:
    from datetime import datetime, timedelta
    from sqlalchemy import and_, outerjoin
    from app.models import DailyKline, Stock

    # 1. Get the latest date available in the system
    latest_date_row = db.query(DailyKline.date).order_by(desc(DailyKline.date)).first()
    if not latest_date_row:
        return []
    
    latest_date_str = latest_date_row[0]
    latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
    
    # 2. Determine start date based on timeframe
    delta, _ = _get_timeframe_delta(timeframe)
    start_date = latest_date - delta
    start_date_str = start_date.strftime("%Y-%m-%d")

    # 3. Subquery for latest prices
    # We want the price at latest_date and the price at or nearest to start_date
    latest_prices = (
        db.query(DailyKline.stock_id, DailyKline.close.label("latest_close"))
        .filter(DailyKline.date == latest_date_str)
        .subquery()
    )

    # For the start price, we find the closest date <= start_date for each stock
    # This is slightly complex in standard SQL without lateral joins, 
    # but we can approximate by getting the max date <= start_date for each stock.
    start_dates_subq = (
        db.query(DailyKline.stock_id, func.max(DailyKline.date).label("max_date"))
        .filter(DailyKline.date <= start_date_str)
        .group_by(DailyKline.stock_id)
        .subquery()
    )

    start_prices = (
        db.query(DailyKline.stock_id, DailyKline.close.label("start_close"))
        .join(start_dates_subq, and_(
            DailyKline.stock_id == start_dates_subq.c.stock_id,
            DailyKline.date == start_dates_subq.c.max_date
        ))
        .subquery()
    )

    # 4. Join everything and calculate change
    query = (
        db.query(Stock)
        .join(latest_prices, Stock.id == latest_prices.c.stock_id)
        .join(start_prices, Stock.id == start_prices.c.stock_id)
        .filter(start_prices.c.start_close > 0)
    )

    # Sort by pct change: ((latest - start) / start) * 100
    pct_change = ((latest_prices.c.latest_close - start_prices.c.start_close) / start_prices.c.start_close) * 100
    
    if sort_desc:
        query = query.order_by(desc(pct_change))
    else:
        query = query.order_by(pct_change)

    return query.limit(limit).all()


def _build_dashboard_item(db: Session, stock: Stock) -> Dict:
    """
    Build a dashboard-style item for a single stock:
    price, change_24h, change_7d, market_cap, volume_24h, sparkline_7d.
    """
    # Latest 8 klines (today + last 7 days for sparkline and change calc)
    klines = (
        db.query(DailyKline)
        .filter(DailyKline.stock_id == stock.id)
        .order_by(desc(DailyKline.date))
        .limit(8)
        .all()
    )

    if not klines:
        return {
            "id": stock.id,
            "symbol": stock.symbol.upper(),
            "name": stock.name,
            "price": None,
            "change_1h": None,
            "change_24h": None,
            "change_7d": None,
            "market_cap": None,
            "volume_24h": None,
            "sparkline_7d": [],
        }

    latest = klines[0]
    prev_24h = klines[1] if len(klines) > 1 else None
    prev_7d = klines[-1] if len(klines) >= 8 else None

    change_24h = None
    if latest.close and prev_24h and prev_24h.close:
        change_24h = ((latest.close - prev_24h.close) / prev_24h.close) * 100

    change_7d = None
    if latest.close and prev_7d and prev_7d.close:
        change_7d = ((latest.close - prev_7d.close) / prev_7d.close) * 100

    ratio = (
        db.query(StockRatio)
        .filter(StockRatio.stock_id == stock.id)
        .order_by(desc(StockRatio.period_ending))
        .first()
    )

    sparkline_data = [
        {"date": str(k.date), "value": float(k.close) if k.close else None}
        for k in reversed(klines[:7])
    ]

    return {
        "id": stock.id,
        "symbol": stock.symbol.upper(),
        "name": stock.name,
        "price": float(latest.close) if latest.close else None,
        "change_1h": None,
        "change_24h": change_24h,
        "change_7d": change_7d,
        "market_cap": float(ratio.market_cap) if ratio and ratio.market_cap else None,
        "volume_24h": float(latest.volume) if latest.volume else None,
        "sparkline_7d": sparkline_data,
    }


def _compute_timeframe_change_pct(db: Session, stock_id: int, timeframe: str) -> Optional[float]:
    """
    Compute percent change over the requested timeframe for a single stock.
    """
    from datetime import datetime
    from app.models import DailyKline
    latest_row = (
        db.query(DailyKline.date, DailyKline.close)
        .filter(DailyKline.stock_id == stock_id)
        .order_by(desc(DailyKline.date))
        .first()
    )
    if not latest_row or not latest_row.close:
        return None
    latest_date_str, latest_close = latest_row
    delta, _ = _get_timeframe_delta(timeframe)
    latest_date = datetime.strptime(latest_date_str, "%Y-%m-%d")
    start_date_str = (latest_date - delta).strftime("%Y-%m-%d")
    start_date_row = (
        db.query(func.max(DailyKline.date))
        .filter(DailyKline.stock_id == stock_id, DailyKline.date <= start_date_str)
        .first()
    )
    if not start_date_row or not start_date_row[0]:
        return None
    start_row = (
        db.query(DailyKline.close)
        .filter(DailyKline.stock_id == stock_id, DailyKline.date == start_date_row[0])
        .first()
    )
    if not start_row or not start_row[0]:
        return None
    start_close = start_row[0]
    if not start_close or start_close == 0:
        return None
    return ((latest_close - start_close) / start_close) * 100.0


def get_top_gainers_dashboard(db: Session, limit: int = 10, timeframe: str = "1d") -> List[Dict]:
    """
    Top gainers formatted like dashboard items.
    Stocks are selected by timeframe-based move, then enriched with dashboard fields.
    """
    stocks = _get_top_moved_stocks(db, limit, timeframe, sort_desc=True)
    items: List[Dict] = []
    for s in stocks:
        it = _build_dashboard_item(db, s)
        if timeframe != "1d":
            pct = _compute_timeframe_change_pct(db, s.id, timeframe)
            if pct is not None:
                it["change_24h"] = pct
                if timeframe == "1w":
                    it["change_7d"] = pct
        items.append(it)
    return items


def get_top_losers_dashboard(db: Session, limit: int = 10, timeframe: str = "1d") -> List[Dict]:
    """
    Top losers formatted like dashboard items.
    Stocks are selected by timeframe-based move, then enriched with dashboard fields.
    """
    stocks = _get_top_moved_stocks(db, limit, timeframe, sort_desc=False)
    items: List[Dict] = []
    for s in stocks:
        it = _build_dashboard_item(db, s)
        if timeframe != "1d":
            pct = _compute_timeframe_change_pct(db, s.id, timeframe)
            if pct is not None:
                it["change_24h"] = pct
                if timeframe == "1w":
                    it["change_7d"] = pct
        items.append(it)
    return items


# ════════════════════════════════════════════════════════════════════════════
#  Technical Analysis & Scoring Helpers
# ════════════════════════════════════════════════════════════════════════════


def _calculate_technical_indicators(df: pd.DataFrame) -> Dict:
    """
    Calculate all Investing.com style indicators for the latest candle in the DataFrame.
    Expects df with columns: [open, high, low, close, volume]
    """
    if df.empty or len(df) < 30:
        return {}

    close = df['close']
    ma_periods = [5, 10, 20, 50, 100, 200]
    mas = {}
    for p in ma_periods:
        if len(df) >= p:
            sma = ta.trend.sma_indicator(close, window=p).iloc[-1]
            ema = ta.trend.ema_indicator(close, window=p).iloc[-1]
            last_price = close.iloc[-1]
            mas[f'sma_{p}'] = {"value": sma, "signal": "Buy" if last_price > sma else "Sell"}
            mas[f'ema_{p}'] = {"value": ema, "signal": "Buy" if last_price > ema else "Sell"}

    indicators = {}
    rsi_val = ta.momentum.rsi(close, window=14).iloc[-1]
    rsi_signal = "Neutral"
    if rsi_val > 70: rsi_signal = "Sell"
    elif rsi_val < 30: rsi_signal = "Buy"
    indicators['RSI(14)'] = {"value": rsi_val, "signal": rsi_signal}

    stoch_k = ta.momentum.stoch(df['high'], df['low'], close, window=9, smooth_window=6).iloc[-1]
    stoch_signal = "Neutral"
    if stoch_k > 80: stoch_signal = "Sell"
    elif stoch_k < 20: stoch_signal = "Buy"
    indicators['STOCH(9,6)'] = {"value": stoch_k, "signal": stoch_signal}

    macd_val = ta.trend.macd_diff(close).iloc[-1]
    indicators['MACD(12,26)'] = {"value": macd_val, "signal": "Buy" if macd_val > 0 else "Sell"}

    adx_val = ta.trend.adx(df['high'], df['low'], close).iloc[-1]
    indicators['ADX(14)'] = {"value": adx_val, "signal": "Neutral"}

    wr = ta.momentum.williams_r(df['high'], df['low'], close).iloc[-1]
    wr_signal = "Neutral"
    if wr < -80: wr_signal = "Buy"
    elif wr > -20: wr_signal = "Sell"
    indicators['Williams %R'] = {"value": wr, "signal": wr_signal}

    cci = ta.trend.cci(df['high'], df['low'], close).iloc[-1]
    cci_signal = "Neutral"
    if cci > 100: cci_signal = "Sell"
    elif cci < -100: cci_signal = "Buy"
    indicators['CCI(14)'] = {"value": cci, "signal": cci_signal}

    buy_count = 0
    sell_count = 0
    for m in mas.values():
        if m['signal'] == "Buy": buy_count += 1
        elif m['signal'] == "Sell": sell_count += 1
    for i in indicators.values():
        if i['signal'] == "Buy": buy_count += 1
        elif i['signal'] == "Sell": sell_count += 1
    
    total = buy_count + sell_count
    if total == 0: summary = "Neutral"
    elif buy_count / total > 0.8: summary = "Strong Buy"
    elif buy_count / total > 0.6: summary = "Buy"
    elif sell_count / total > 0.8: summary = "Strong Sell"
    elif sell_count / total > 0.6: summary = "Sell"
    else: summary = "Neutral"

    return {
        "summary": summary,
        "indicators": [schemas.TechnicalIndicatorSignal(name=k, value=float(v['value']), signal=v['signal']) for k, v in indicators.items()],
        "moving_averages": [schemas.MovingAverageSignal(period=int(k.split('_')[1]), value=float(v['value']), signal=v['signal']) for k, v in mas.items() if 'sma' in k]
    }


def _calculate_fair_value(db: Session, stock: Stock) -> Dict:
    """Blended valuation model: P/E Mean + DCF Lite."""
    ratio = _get_latest_ratio(db, stock.id)
    income = db.query(IncomeStatement).filter(IncomeStatement.stock_id == stock.id).order_by(desc(IncomeStatement.period_ending)).first()
    
    if not ratio or not income or not ratio.last_close_price:
        return {"fair_value": None, "valuation_status": "Insufficient Data", "upside_potential": None}

    eps = float(income.eps_basic or 0)
    pe_valuation = eps * 10
    
    fcf = float(income.free_cash_flow or 0)
    if fcf > 0:
        r = 0.15 
        g = 0.05 
        dcf_valuation = (fcf * (1 + g)) / (r - g)
        shares = float(income.shares_basic or 1)
        dcf_ps = dcf_valuation / shares
        fair_value = (pe_valuation + dcf_ps) / 2
    else:
        fair_value = pe_valuation

    current_price = float(ratio.last_close_price)
    upside = ((fair_value - current_price) / current_price) * 100 if current_price > 0 else 0.0
    
    status = "Fair Value"
    if upside > 20: status = "Undervalued"
    elif upside < -20: status = "Overvalued"

    return {
        "fair_value": fair_value,
        "valuation_status": status,
        "upside_potential": upside
    }


def _calculate_health_score(db: Session, stock: Stock) -> Dict:
    """Score 1-5 based on Solvency, Profitability, and Liquidity."""
    ratio = _get_latest_ratio(db, stock.id)
    if not ratio:
        return {"score": 0, "status": "Insufficient Data"}

    s_score = 0 
    if ratio.debt_equity and ratio.debt_equity < 1: s_score += 1
    if ratio.interest_coverage and ratio.interest_coverage > 3: s_score += 1

    p_score = 0 
    if ratio.roe and ratio.roe > 0.15: p_score += 1
    if ratio.roa and ratio.roa > 0.05: p_score += 1

    l_score = 0 
    if ratio.current_ratio and ratio.current_ratio > 1.5: l_score += 1

    total_score = s_score + p_score + l_score
    status_map = {0: "Risky", 1: "Poor", 2: "Fair", 3: "Good", 4: "Good", 5: "Excellent"}
    
    return {
        "score": max(1, total_score),
        "status": status_map.get(total_score, "Fair"),
        "solvency_score": float(s_score),
        "profitability_score": float(p_score),
        "liquidity_score": float(l_score)
    }


def get_market_suggestions(db: Session) -> List[Dict]:
    """
    Generate trending topics and analysis questions based on current market state.
    """
    suggestions = []
    
    # 1. Top Gainer Trend
    top_gainer = (
        db.query(Stock)
        .join(DailyKline)
        .order_by(desc(DailyKline.close / DailyKline.open)) # Simplistic 1d jump
        .first()
    )
    if top_gainer:
        suggestions.append({
            "id": f"gainer_{top_gainer.id}",
            "text": f"Why is {top_gainer.symbol} rallying today?",
            "type": "trend",
            "icon": "trending_up"
        })

    # 2. Market Sentiment Focus
    suggestions.append({
        "id": "sentiment_analysis",
        "text": "Analyze current NGX market sentiment",
        "type": "question",
        "icon": "psychology"
    })

    # 3. Dividend Alert
    recent_div = (
        db.query(Dividend)
        .join(Stock)
        .order_by(desc(Dividend.ex_dividend_date))
        .first()
    )
    if recent_div:
        suggestions.append({
            "id": f"div_{recent_div.id}",
            "text": f"Upcoming dividend for {recent_div.stock.symbol}",
            "type": "alert",
            "icon": "paid"
        })

    # Fallback to general questions if list is short
    if len(suggestions) < 5:
        suggestions.append({"id": "q1", "text": "Top dividend stocks for March", "type": "trend", "icon": "calendar_month"})
        suggestions.append({"id": "q2", "text": "Which sectors are outperforming?", "type": "question", "icon": "donut_small"})

    return suggestions
