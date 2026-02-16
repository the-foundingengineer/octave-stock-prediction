from pydantic import BaseModel
from typing import List, Optional

class StockRecordBase(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None
    symbol: str

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

class KlineData(BaseModel):
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float

class KlineResponse(BaseModel):
    stock_id: int
    symbol: str
    interval: str
    klines: List[KlineData]

    class Config:
        orm_mode = True

class StockStatsResponse(BaseModel):
    stock_id: int
    symbol: str
    market_cap: Optional[float] = None
    revenue_ttm: Optional[float] = None
    net_income: Optional[float] = None
    eps: Optional[float] = None
    shares_outstanding: Optional[int] = None
    pe_ratio: Optional[float] = None
    forward_pe: Optional[float] = None
    dividend: Optional[float] = None
    ex_dividend_date: Optional[str] = None
    volume: Optional[int] = None
    avg_volume: Optional[int] = None
    open: Optional[float] = None
    previous_close: Optional[float] = None
    day_range: Optional[str] = None
    fifty_two_week_range: Optional[str] = None
    beta: Optional[float] = None
    rsi: Optional[float] = None
    earnings_date: Optional[str] = None

    class Config:
        orm_mode = True

class StockInfoResponse(BaseModel):
    stock_id: int
    symbol: str
    ipo_date: Optional[str] = None
    name: Optional[str] = None
    fifty_two_week_high: Optional[float] = None
    fifty_two_week_low: Optional[float] = None
    fifty_day_moving_average: Optional[float] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    sentiment: Optional[str] = None
    sp_score: Optional[int] = None
    
    class Config:
        orm_mode = True


class StockRelatedResponse(BaseModel):
    stock_id: int
    symbol: str
    market_cap: Optional[float] = None
    revenue_ttm: Optional[float] = None

    class Config:
        orm_mode = True

