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
    Fetches data for a stock and updates only the sector in the database.
    """
    data = fetch_stock_data(symbol, api_token)
    
    if data.get("code") != 0:
        raise Exception(f"API Error: {data.get('msg')}")
        
    stock_data = data.get("data", {})
    
    # Check if stock exists
    db_stock = db.query(models.Stock).filter(models.Stock.symbol == symbol).first()
    
    if not db_stock:
        # Create new stock if it doesn't exist
        db_stock = models.Stock(symbol=symbol)
        db.add(db_stock)
    
    # Update only sector
    db_stock.sector = stock_data.get("s")
    
    db_stock.last_updated = datetime.datetime.utcnow()
    
    db.commit()
    db.refresh(db_stock)
    return db_stock
