from pydantic import BaseModel

class StockBase(BaseModel):
    date: str
    open: str
    high: str
    low: str
    close: str
    volume: str
    stock_name: str

class StockCreate(StockBase):
    pass

class Stock(StockBase):
    id: int

    class Config:
        orm_mode = True
