from sqlalchemy.orm import Session
from sqlalchemy import desc, func, text as sa_text
from app.models import StockRecord, Stock
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
    db_stock = StockRecord(
        date=stock.date,
        open=stock.open,
        high=stock.high,
        low=stock.low,
        close=stock.close,
        volume=stock.volume,
        stock_name=stock.stock_name
    )
    db.add(db_stock)
    db.commit()
    db.refresh(db_stock)
    return db_stock


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


def get_market_table(db: Session, page: int, limit: int):
    """
    Compute a market-table summary for each stock.
    Returns price (latest close), 24h% (day-over-day), 7d%, 
    market cap, volume, and outstanding stock.
    """
    offset = (page - 1) * limit
    stocks = (
        db.query(Stock)
        .order_by(Stock.id)
        .offset(offset)
        .limit(limit)
        .all()
    )

    result = []
    for stock in stocks:
        # Fetch the latest 2 records for this stock (for price + 24h%)
        latest_records = (
            db.query(StockRecord)
            .filter(StockRecord.stock_name == stock.symbol)
            .order_by(desc(func.to_date(StockRecord.date, 'MM/DD/YYYY')))
            .limit(8)  # grab enough for 7d calculation
            .all()
        )

        if not latest_records:
            continue

        # Latest record
        today = latest_records[0]
        price = _safe_float(today.close)
        volume_24h = _parse_volume(today.volume)

        # 24h % (day-over-day)
        change_24h = None
        if len(latest_records) >= 2:
            yesterday_close = _safe_float(latest_records[1].close)
            if yesterday_close and yesterday_close != 0:
                change_24h = round(((price - yesterday_close) / yesterday_close) * 100, 2)

        # 7d % (compare to ~7th record back)
        change_7d = None
        if len(latest_records) >= 6:
            week_ago_close = _safe_float(latest_records[-1].close)
            if week_ago_close and week_ago_close != 0:
                change_7d = round(((price - week_ago_close) / week_ago_close) * 100, 2)

        # Market cap
        market_cap = None
        if stock.outstanding_shares and price:
            market_cap = round(price * stock.outstanding_shares, 2)

        result.append({
            "id": stock.id,
            "name": stock.name or stock.symbol,
            "symbol": stock.symbol,
            "price": price,
            "change_24h": change_24h,
            "change_7d": change_7d,
            "market_cap": market_cap,
            "volume_24h": volume_24h,
            "outstanding_stock": stock.outstanding_shares,
        })

    return result


def _safe_float(val) -> float:
    """Safely convert a string to float, returning 0.0 on failure."""
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0
