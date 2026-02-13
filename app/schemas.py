from pydantic import BaseModel
from typing import List, Optional

class StockRecordBase(BaseModel):
    date: str
    open: str
    high: str
    low: str
    close: str
    volume: str
    stock_name: str

class StockRecordCreate(StockRecordBase):
    pass

class StockRecord(StockRecordBase):
    id: int

    class Config:
        orm_mode = True

class StockBase(BaseModel):
    symbol: str

class StockCreate(StockBase):
    pass

class Stock(StockBase):
    id: int
    name: Optional[str] = None
    outstanding_shares: Optional[int] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    description: Optional[str] = None
    website: Optional[str] = None
    market_cap: Optional[str] = None
    currency: Optional[str] = None
    exchange: Optional[str] = None
    last_updated: Optional[str] = None
    pe_ratio: Optional[str] = None
    fifty_two_week_high: Optional[str] = None
    fifty_two_week_low: Optional[str] = None
    adjustment_factor: Optional[str] = None

    class Config:
        orm_mode = True

class MarketTableItem(BaseModel):
    id: int
    name: str
    symbol: str
    price: float
    change_24h: Optional[float] = None
    change_7d: Optional[float] = None
    market_cap: Optional[float] = None
    volume_24h: float
    outstanding_stock: Optional[int] = None

    class Config:
        orm_mode = True

class unique_name(BaseModel):
    id: int
    stock_name: str

    class Config:
        orm_mode = True

class StockSignal(BaseModel):
    symbol: str
    signal: str
    score: int
    reasons: List[str]

    class Config:
        orm_mode = True
