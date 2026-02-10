from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.database import Base

class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, unique=True, index=True)
    name = Column(String, nullable=True)
    outstanding_shares = Column(Integer, nullable=True)

class StockRecord(Base):
    __tablename__ = "stock_records"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(String, index=True)
    open = Column(String, index=True)
    high = Column(String, index=True)
    low = Column(String, index=True)
    close = Column(String, index=True)
    volume = Column(String, index=True)
    stock_name = Column(String, index=True)
    # Ideally link to Stock, but for now we keep independent as we verify