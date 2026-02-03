import models, schemas
from sqlalchemy import Column, Integer, String
from database import Base

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
