import requests
from sqlalchemy.orm import Session
from app import models, schemas
import datetime

def fetch_stock_data(symbol: str, api_token: str) -> dict:
    """
    Fetches stock data from the iTick API.
    """
    url = "https://api-free.itick.org/stock/info"
    params = {
        "type": "stock",
        "region": "NG",
        "code": symbol
    }
    headers = {
        "accept": "application/json",
        "token": api_token
    }

    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    return response.json()

def update_stock_info(db: Session, symbol: str, api_token: str):
    """
    Fetches data for a stock and updates the database.
    """
    data = fetch_stock_data(symbol, api_token)
    
    if data.get("code") != 0:
        raise Exception(f"API Error: {data.get('msg')}")
        
    stock_data = data.get("data", {})
    
    # Check if stock exists
    db_stock = db.query(models.Stock).filter(models.Stock.symbol == symbol).first()
    
    if not db_stock:
        # Create new stock if it doesn't exist (though ideally we should have a base list)
        db_stock = models.Stock(symbol=symbol)
        db.add(db_stock)
    
    # Update fields
    db_stock.name = stock_data.get("n")
    db_stock.sector = stock_data.get("s")
    db_stock.industry = stock_data.get("i")
    db_stock.description = stock_data.get("bd")
    db_stock.website = stock_data.get("wu")
    db_stock.market_cap = str(stock_data.get("mcb"))
    db_stock.outstanding_shares = stock_data.get("tso")
    db_stock.currency = stock_data.get("r")
    db_stock.exchange = stock_data.get("e")
    db_stock.pe_ratio = str(stock_data.get("pet"))
    db_stock.fifty_two_week_high = str(stock_data.get("ph52"))
    db_stock.fifty_two_week_low = str(stock_data.get("pl52"))
    
    db_stock.last_updated = datetime.datetime.now().isoformat()
    
    db.commit()
    db.refresh(db_stock)
    return db_stock
