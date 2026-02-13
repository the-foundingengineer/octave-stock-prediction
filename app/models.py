from sqlalchemy import Column, Integer, String, ForeignKey, BigInteger, Float, UniqueConstraint
from sqlalchemy.orm import relationship
from app.database import Base

class Stock(Base):
    __tablename__ = "stocks"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, unique=True, index=True)
    name = Column(String, nullable=True)
    outstanding_shares = Column(BigInteger, nullable=True)
    sector = Column(String, nullable=True)
    industry = Column(String, nullable=True)
    description = Column(String, nullable=True)
    website = Column(String, nullable=True)
    market_cap = Column(String, nullable=True)
    currency = Column(String, nullable=True)
    exchange = Column(String, nullable=True)
    last_updated = Column(String, nullable=True)
    pe_ratio = Column(String, nullable=True) # "pet"
    fifty_two_week_high = Column(String, nullable=True) # "ph52"
    fifty_two_week_low = Column(String, nullable=True) # "pl52"
    adjustment_factor = Column(String, nullable=True) # from stock/split "v"

    daily_klines = relationship("DailyKline", back_populates="stock")

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

class DailyKline(Base):
    __tablename__ = "daily_klines"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, ForeignKey("stocks.symbol"), index=True)
    date = Column(String, index=True)           # YYYY-MM-DD
    timestamp = Column(BigInteger, nullable=True) # epoch ms from API
    open = Column(Float, nullable=True)
    high = Column(Float, nullable=True)
    low = Column(Float, nullable=True)
    close = Column(Float, nullable=True)
    volume = Column(BigInteger, nullable=True)
    turnover = Column(Float, nullable=True)

    stock = relationship("Stock", back_populates="daily_klines")

    __table_args__ = (
        UniqueConstraint("symbol", "date", name="uix_symbol_date"),
    )
