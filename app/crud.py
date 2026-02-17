from sqlalchemy.orm import Session
from sqlalchemy import desc, func, text as sa_text
from app.models import DailyKline, Stock, IncomeStatement, BalanceSheet, StockRatio
from app.schemas import StockRecordCreate

def get_stock(db: Session, stock_id: int):
    return db.query(Stock).filter(Stock.id == stock_id).first()

def get_stocks(db: Session, page: int, limit: int):
    offset = (page - 1) * limit
    return (
        db.query(Stock)
        .order_by(Stock.id)
        .offset(offset)
        .limit(limit)
        .all()
    )

def get_unique_stock_names(db: Session):
    results = db.query(Stock).all()
    return [{"id": stock.id, "stock_name": stock.symbol.upper()} for stock in results]

def create_stock_record(db: Session, stock: StockRecordCreate):
    db_stock = DailyKline(
        date=stock.date,
        open=stock.open,
        high=stock.high,
        low=stock.low,
        close=stock.close,
        volume=stock.volume,
        symbol=stock.symbol
    )
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock

def get_stock_kline(db: Session, stock_id: int, interval: str, limit: int):
    """
    Fetch and aggregate klines for various intervals using stock_id.
    """
    from datetime import datetime
    
    stock = db.query(Stock).filter(Stock.id == stock_id).first()
    if not stock:
        return None
    
    symbol = stock.symbol.upper()
    
    # Standardize interval
    interval = interval.lower()
    if interval in ['1d', 'daily', 'day']:
        agg_key = 'day'
    elif interval in ['1w', 'weekly', 'week']:
        agg_key = 'week'
    elif interval in ['1m', 'monthly', 'month']:
        agg_key = 'month'
    elif interval in ['1y', 'yearly', 'year']:
        agg_key = 'year'
    else:
        agg_key = 'day' # Default

    # If limit=50 and interval=week, we need at least 50 * 5 = 250 daily records.
    fetch_limit = limit
    if agg_key == 'week': fetch_limit = limit * 7
    elif agg_key == 'month': fetch_limit = limit * 31
    elif agg_key == 'year': fetch_limit = limit * 366
    
    # Cap fetch_limit at 5000 to prevent OOM
    fetch_limit = min(fetch_limit, 5000)

    query = db.query(DailyKline).filter(DailyKline.symbol == symbol.upper())
    daily_results = query.order_by(DailyKline.date.asc()).all() # Sort ASC for easier aggregation
    
    if not daily_results:
        return {"symbol": symbol.upper(), "interval": interval, "klines": []}

    if agg_key == 'day':
        # Daily is just formatting the latest N
        results = daily_results[-limit:] if len(daily_results) > limit else daily_results
        results.reverse() # Show latest first
        formatted = []
        for r in results:
            formatted.append({
                "date": str(r.date),
                "open": _safe_float(r.open),
                "high": _safe_float(r.high),
                "low": _safe_float(r.low),
                "close": _safe_float(r.close),
                "volume": _safe_float(r.volume)
            })
        return {"symbol": symbol.upper(), "interval": interval, "klines": formatted}

    # Helper to get grouping key
    def get_group_key(date_str):
        # Handle both %Y-%m-%d and potentially other formats if needed, but standardize on %Y-%m-%d
        try:
            dt = datetime.strptime(date_str, '%Y-%m-%d')
        except ValueError:
            # Fallback for MM/DD/YYYY if present in DB
            dt = datetime.strptime(date_str, '%m/%d/%Y')
        
        if agg_key == 'week':
            # Use ISO week: (year, week_number)
            isocal = dt.isocalendar()
            return f"{isocal[0]}-W{isocal[1]:02d}"
        elif agg_key == 'month':
            return dt.strftime('%Y-%m')
        elif agg_key == 'year':
            return dt.strftime('%Y')
        return date_str

    # Aggregate
    groups = {}
    group_order = []
    
    for r in daily_results:
        # Skip records with missing essential data to avoid float(None) errors
        if r.open is None or r.high is None or r.low is None or r.close is None:
            continue
            
        key = get_group_key(r.date)
        r_open = _safe_float(r.open)
        r_high = _safe_float(r.high)
        r_low = _safe_float(r.low)
        r_close = _safe_float(r.close)
        r_vol = _safe_float(r.volume or 0)

        if key not in groups:
            groups[key] = {
                'date': r.date, 
                'open': r_open,
                'high': r_high,
                'low': r_low,
                'close': r_close,
                'volume': r_vol
            }
            group_order.append(key)
        else:
            g = groups[key]
            if r_high > g['high']: g['high'] = r_high
            if r_low < g['low']: g['low'] = r_low
            g['close'] = r_close
            g['volume'] += r_vol
    
    # Format and apply limit
    final_results = []
    for key in reversed(group_order):
        g = groups[key]
        final_results.append({
            "date": str(g['date']),
            "open": g['open'],
            "high": g['high'],
            "low": g['low'],
            "close": g['close'],
            "volume": g['volume']
        })
        if len(final_results) >= limit:
            break
            
    return {
        "stock_id": stock_id,
        "symbol": symbol,
        "interval": interval,
        "klines": final_results
    }

def get_stock_stats(db: Session, stock_id: int):
    """
    Aggregate comprehensive stats for a stock using stock_id.
    """
    stock = db.query(Stock).filter(Stock.id == stock_id).first()
    if not stock:
        return None
    
    symbol = stock.symbol.upper()

    # Latest 2 Klines for Price, Change, Previous Close, Ranges, RSI, etc.
    latest_klines = (
        db.query(DailyKline)
        .filter(DailyKline.symbol == symbol)
        .order_by(desc(DailyKline.date))
        .limit(2)
        .all()
    )
    
    latest = latest_klines[0] if latest_klines else None
    prev = latest_klines[1] if len(latest_klines) > 1 else None

    # Latest TTM Income Statement
    income = (
        db.query(IncomeStatement)
        .filter(IncomeStatement.stock_id == stock.id, IncomeStatement.period_type == 'TTM')
        .order_by(desc(IncomeStatement.period_ending))
        .first()
    )
    if not income: # Fallback to FY if TTM missing
         income = (
            db.query(IncomeStatement)
            .filter(IncomeStatement.stock_id == stock.id)
            .order_by(desc(IncomeStatement.period_ending))
            .first()
        )

    # Latest Balance Sheet
    balance = (
        db.query(BalanceSheet)
        .filter(BalanceSheet.stock_id == stock.id)
        .order_by(desc(BalanceSheet.period_ending))
        .first()
    )

    # Day's Range calculation
    day_range = None
    if latest and latest.low and latest.high:
        day_range = f"{latest.low:,.2f} - {latest.high:,.2f}"

    # 52-Week Range
    fifty_two_range = None
    if latest and latest.week_52_low and latest.week_52_high:
        fifty_two_range = f"{latest.week_52_low:,.2f} - {latest.week_52_high:,.2f}"

    return {
        "stock_id": stock_id,
        "symbol": symbol,
        "market_cap": float(latest.market_cap) if latest and latest.market_cap else None,
        "revenue_ttm": float(income.revenue) if income and income.revenue else None,
        "net_income": float(income.net_income) if income and income.net_income else None,
        "eps": float(income.eps_basic) if income and income.eps_basic else None,
        "shares_outstanding": balance.shares_outstanding if balance and balance.shares_outstanding else None,
        "pe_ratio": latest.pe_ratio if latest and latest.pe_ratio else None,
        "forward_pe": latest.forward_pe if latest and latest.forward_pe else None,
        "dividend": latest.dividend_per_share if latest and latest.dividend_per_share else None,
        "ex_dividend_date": latest.ex_dividend_date if latest and latest.ex_dividend_date else None,
        "volume": latest.volume if latest and latest.volume else None,
        "avg_volume": latest.avg_volume_20d if latest and latest.avg_volume_20d else None,
        "open": latest.open if latest and latest.open else None,
        "previous_close": prev.close if prev and prev.close else None,
        "day_range": day_range,
        "fifty_two_week_range": fifty_two_range,
        "beta": latest.beta if latest and latest.beta else None,
        "rsi": latest.rsi if latest and latest.rsi else None,
        "earnings_date": None # Not captured in current specific models but field is there for future
    }

def get_stock_info(db: Session, stock_id: int):
    stock = db.query(Stock).filter(Stock.id == stock_id).first()
    if not stock:
        return None
    
    symbol = stock.symbol.upper()

    latest_klines = (
        db.query(DailyKline)
        .filter(DailyKline.symbol == symbol)
        .order_by(desc(DailyKline.date))
        .limit(2)
        .all()
    )
    
    latest = latest_klines[0] if latest_klines else None
    prev = latest_klines[1] if len(latest_klines) > 1 else None


    return {
        "stock_id": stock_id,
        "symbol": symbol,
        "ipo_date": getattr(stock, 'founded', None),
        "name": stock.name,
        "fifty_two_week_high": _safe_float(latest.week_52_high) if latest else None,
        "fifty_two_week_low": _safe_float(latest.week_52_low) if latest else None,
        "fifty_day_moving_average": _safe_float(latest.ma_50d) if latest else None,
        "sector": stock.sector,
        "industry": stock.industry,
        "sentiment": getattr(stock, 'sentiment', None),
        "sp_score": getattr(stock, 'sp_score', None),
    }

    
def get_stock_related(db: Session, stock_id: int, limit: int = 10):
    stock = db.query(Stock).filter(Stock.id == stock_id).first()
    if not stock:
        return None
    
    # Fetch related stocks in the same sector, excluding the current one
    related_stocks = (
        db.query(Stock)
        .filter(Stock.sector == stock.sector)
        .filter(Stock.id != stock_id)
        .limit(limit)
        .all()
    )

    results = []
    for s in related_stocks:
        # Fetch latest metrics for each related stock
        # Using similar logic to get_stock_stats but simplified
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
            "market_cap": float(latest_kline.market_cap) if latest_kline and latest_kline.market_cap else None,
            "revenue_ttm": float(latest_income.revenue) if latest_income and latest_income.revenue else None,
        })

    return results


def _parse_volume(vol_str: str) -> float:
    """Parse volume strings like '1.05M', '939.04K', '0.00K' into floats."""
    if not vol_str:
        return 0.0
    vol_str = vol_str.strip().upper()
    try:
        if vol_str.endswith("M"):
            return float(vol_str[:-1]) * 1_000_000
        elif vol_str.endswith("K"):
            return float(vol_str[:-1]) * 1_000
        elif vol_str.endswith("B"):
            return float(vol_str[:-1]) * 1_000_000_000
        else:
            return float(vol_str.replace(",", ""))
    except (ValueError, TypeError):
        return 0.0




def _safe_float(val) -> float:
    """Safely convert a string to float, returning 0.0 on failure."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
