from sqlalchemy.orm import Session
from sqlalchemy import desc, func, select, and_, text as sa_text
from app.models import DailyKline, Stock, IncomeStatement, BalanceSheet, StockRatio, CashFlow
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
        return {
            "stock_id": stock_id,
            "symbol": symbol.upper(),
            "interval": interval,
            "klines": []
        }

    if agg_key == 'day':
        # Daily is just formatting the latest N
        results = daily_results[-limit:] if len(daily_results) > limit else daily_results
        results.reverse() # Show latest first
        formatted = []
        for r in results:
            if r.open is None or r.high is None or r.low is None or r.close is None:
                continue

            formatted.append({
                "date": str(r.date),
                "open": _safe_float(r.open),
                "high": _safe_float(r.high),
                "low": _safe_float(r.low),
                "close": _safe_float(r.close),
                "volume": _safe_float(r.volume)
            })
        return {
            "stock_id": stock_id,
            "symbol": symbol.upper(),
            "interval": interval,
            "klines": formatted
        }

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


def get_popular_comparisons(db: Session):
    latest_kline_date_sub = db.query(
        DailyKline.symbol,
        func.max(DailyKline.date).label("latest_date")
    ).group_by(DailyKline.symbol).subquery()

    ranked_stocks_sub = db.query(
        Stock.id,
        Stock.symbol,
        Stock.sector,
        func.row_number().over(
            partition_by=Stock.sector,
            order_by=desc(DailyKline.market_cap)
        ).label("rank")
    ).join(DailyKline, Stock.symbol == DailyKline.symbol)\
     .join(
         latest_kline_date_sub,
         (DailyKline.symbol == latest_kline_date_sub.c.symbol) & 
         (DailyKline.date == latest_kline_date_sub.c.latest_date)
     ).subquery()

    # Query subquery columns explicitly
    top_stocks = db.query(
        ranked_stocks_sub.c.id,
        ranked_stocks_sub.c.symbol,
        ranked_stocks_sub.c.sector,
        ranked_stocks_sub.c.rank
    ).filter(ranked_stocks_sub.c.rank <= 2).all()

    return [
        {
            "id": s.id,
            "symbol": s.symbol,
            "sector": s.sector,
            "rank": s.rank
        } for s in top_stocks
    ]

def get_stock_comparison_details(db: Session, stock_id: int):
    stock = db.query(Stock).filter(Stock.id == stock_id).first()
    if not stock:
        return None
    return _get_stock_comparison_data(db, stock.id, stock.symbol)

def _get_stock_comparison_data(db: Session, stock_id: int, symbol: str):
    # Fetch data from all tables
    stock = db.query(Stock).filter(Stock.id == stock_id).first()
    
    # Latest 2 Klines for 1D Change
    klines = db.query(DailyKline).filter(DailyKline.symbol == symbol).order_by(desc(DailyKline.date)).limit(2).all()
    latest = klines[0] if klines else None
    prev = klines[1] if len(klines) > 1 else None
    
    # Latest Financials
    income = db.query(IncomeStatement).filter(IncomeStatement.stock_id == stock_id).order_by(desc(IncomeStatement.period_ending)).first()
    balance = db.query(BalanceSheet).filter(BalanceSheet.stock_id == stock_id).order_by(desc(BalanceSheet.period_ending)).first()
    cash = db.query(CashFlow).filter(CashFlow.stock_id == stock_id).order_by(desc(CashFlow.period_ending)).first()
    ratio = db.query(StockRatio).filter(StockRatio.stock_id == stock_id).order_by(desc(StockRatio.period_ending)).first()

    # Calculate 1D Change
    p_change_1d = None
    p_change_pct_1d = None
    if latest and prev and latest.close and prev.close:
        p_change_1d = float(latest.close - prev.close)
        p_change_pct_1d = float((latest.close - prev.close) / prev.close * 100)

    # Build comparison item
    return {
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
        
        "stock_price": float(latest.close) if latest and latest.close else None,
        "price_change_1d": p_change_1d,
        "price_change_percent_1d": p_change_pct_1d,
        "open_price": float(latest.open) if latest and latest.open else None,
        "previous_close": float(prev.close) if prev and prev.close else None,
        "low_price": float(latest.low) if latest and latest.low else None,
        "high_price": float(latest.high) if latest and latest.high else None,
        "volume": latest.volume if latest else None,
        "dollar_volume": float(latest.volume * latest.close) if latest and latest.volume and latest.close else None,
        "stock_price_date": latest.date if latest else None,
        
        "fifty_two_week_low": latest.week_52_low if latest else None,
        "fifty_two_week_high": latest.week_52_high if latest else None,
        
        "market_cap": float(latest.market_cap) if latest and latest.market_cap else None,
        "enterprise_value": float(latest.enterprise_value) if latest and latest.enterprise_value else None,
        "pe_ratio": latest.pe_ratio if latest else None,
        "forward_pe": latest.forward_pe if latest else None,
        "ps_ratio": latest.ps_ratio if latest else None,
        "pb_ratio": latest.pb_ratio if latest else None,
        "peg_ratio": None, 
        "ev_sales": float(ratio.ev_sales) if ratio and ratio.ev_sales else None,
        "ev_ebitda": float(ratio.ev_ebitda) if ratio and ratio.ev_ebitda else None,
        "ev_ebit": float(ratio.ev_ebit) if ratio and ratio.ev_ebit else None,
        "ev_fcf": float(ratio.ev_fcf) if ratio and ratio.ev_fcf else None,
        "earnings_yield": float(ratio.earnings_yield) if ratio and ratio.earnings_yield else None,
        "fcf_yield": float(ratio.fcf_yield) if ratio and ratio.fcf_yield else None,
        
        "revenue": float(income.revenue) if income and income.revenue else None,
        "gross_profit": float(income.gross_profit) if income and income.gross_profit else None,
        "operating_income": float(income.operating_income) if income and income.operating_income else None,
        "net_income": float(income.net_income) if income and income.net_income else None,
        "ebitda": float(income.ebitda) if income and income.ebitda else None,
        "ebit": float(income.ebit) if income and income.ebit else None,
        "eps": float(income.eps_basic) if income and income.eps_basic else None,
        "revenue_growth": float(income.revenue_growth_yoy) if income and income.revenue_growth_yoy else None,
        "net_income_growth": float(income.net_income_growth_yoy) if income and income.net_income_growth_yoy else None,
        "eps_growth": float(income.eps_growth_yoy) if income and income.eps_growth_yoy else None,
        
        "gross_margin": float(income.gross_margin) if income and income.gross_margin else None,
        "operating_margin": float(income.operating_margin) if income and income.operating_margin else None,
        "profit_margin": float(income.profit_margin) if income and income.profit_margin else None,
        "fcf_margin": float(income.fcf_margin) if income and income.fcf_margin else None,
        
        "operating_cash_flow": float(cash.operating_cash_flow) if cash and cash.operating_cash_flow else None,
        "investing_cash_flow": float(cash.investing_cash_flow) if cash and cash.investing_cash_flow else None,
        "financing_cash_flow": float(cash.financing_cash_flow) if cash and cash.financing_cash_flow else None,
        "net_cash_flow": float(cash.net_cash_flow) if cash and cash.net_cash_flow else None,
        "capital_expenditures": float(cash.capex) if cash and cash.capex else None,
        "free_cash_flow": float(cash.free_cash_flow) if cash and cash.free_cash_flow else None,
        
        "total_cash": float(balance.cash_and_st_investments) if balance and balance.cash_and_st_investments else None,
        "total_debt": float(balance.total_debt) if balance and balance.total_debt else None,
        "net_cash_debt": float(balance.net_cash_debt) if balance and balance.net_cash_debt else None,
        "total_assets": float(balance.total_assets) if balance and balance.total_assets else None,
        "total_liabilities": float(balance.total_liabilities) if balance and balance.total_liabilities else None,
        "shareholders_equity": float(balance.shareholders_equity) if balance and balance.shareholders_equity else None,
        "working_capital": float(balance.working_capital) if balance and balance.working_capital else None,
        "book_value_per_share": float(balance.book_value_per_share) if balance and balance.book_value_per_share else None,
        "shares_outstanding": balance.shares_outstanding if balance else None,
        
        "roe": float(ratio.roe) if ratio and ratio.roe else None,
        "roa": float(ratio.roa) if ratio and ratio.roa else None,
        "roic": float(ratio.roic) if ratio and ratio.roic else None,
        "roce": float(ratio.roce) if ratio and ratio.roce else None,
        "current_ratio": float(ratio.current_ratio) if ratio and ratio.current_ratio else None,
        "quick_ratio": float(ratio.quick_ratio) if ratio and ratio.quick_ratio else None,
        "debt_equity": float(ratio.debt_equity) if ratio and ratio.debt_equity else None,
        "debt_ebitda": float(ratio.debt_ebitda) if ratio and ratio.debt_ebitda else None,
        "interest_coverage": float(ratio.interest_coverage) if ratio and ratio.interest_coverage else None,
        "altman_z_score": float(ratio.altman_z_score) if ratio and ratio.altman_z_score else None,
        "piotroski_f_score": ratio.piotroski_f_score if ratio else None,
        
        "rsi": latest.rsi if latest else None,
        "beta": latest.beta if latest else None,
        "ma_20": None, 
        "ma_50": latest.ma_50d if latest else None,
        "ma_200": latest.ma_200d if latest else None,
        
        "dividend_yield": float(ratio.dividend_yield) if ratio and ratio.dividend_yield else (latest.dividend_yield if latest else None),
        "dividend_per_share": latest.dividend_per_share if latest else None,
        "ex_div_date": latest.ex_dividend_date if latest else None,
    }

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
