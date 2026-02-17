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


class IncomeStatementResponse(BaseModel):
    id: int
    stock_id: int
    period_ending: str
    period_type: str
    revenue: Optional[float] = None
    operating_revenue: Optional[float] = None
    other_revenue: Optional[float] = None
    revenue_growth_yoy: Optional[float] = None
    cost_of_revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    sga_expenses: Optional[float] = None
    operating_income: Optional[float] = None
    ebitda: Optional[float] = None
    ebit: Optional[float] = None
    interest_expense: Optional[float] = None
    pretax_income: Optional[float] = None
    income_tax: Optional[float] = None
    net_income: Optional[float] = None
    net_income_growth_yoy: Optional[float] = None
    eps_basic: Optional[float] = None
    eps_diluted: Optional[float] = None
    eps_growth_yoy: Optional[float] = None
    dividend_per_share: Optional[float] = None
    shares_basic: Optional[int] = None
    shares_diluted: Optional[int] = None

    class Config:
        orm_mode = True


class StockWithIncomeStatementResponse(BaseModel):
    id: int
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    country: Optional[str] = None
    website: Optional[str] = None
    ceo: Optional[str] = None
    employees: Optional[int] = None
    fiscal_year_end: Optional[str] = None
    income_statement: Optional[IncomeStatementResponse] = None
    website: Optional[str] = None
    country: Optional[str] = None
    employees: Optional[int] = None
    founded: Optional[str] = None
    ipo_date: Optional[str] = None
    
    # Price Data (Latest)
    stock_price: Optional[float] = None
    price_change_1d: Optional[float] = None
    price_change_percent_1d: Optional[float] = None
    open_price: Optional[float] = None
    previous_close: Optional[float] = None
    low_price: Optional[float] = None
    high_price: Optional[float] = None
    volume: Optional[int] = None
    dollar_volume: Optional[float] = None
    stock_price_date: Optional[str] = None
    
    # 52 Week
    fifty_two_week_low: Optional[float] = None
    fifty_two_week_high: Optional[float] = None
    
    # Valuation
    market_cap: Optional[float] = None
    enterprise_value: Optional[float] = None
    pe_ratio: Optional[float] = None
    forward_pe: Optional[float] = None
    ps_ratio: Optional[float] = None
    pb_ratio: Optional[float] = None
    peg_ratio: Optional[float] = None
    ev_sales: Optional[float] = None
    ev_ebitda: Optional[float] = None
    ev_ebit: Optional[float] = None
    ev_fcf: Optional[float] = None
    earnings_yield: Optional[float] = None
    fcf_yield: Optional[float] = None
    
    # Financials (TTM/Recent)
    revenue: Optional[float] = None
    gross_profit: Optional[float] = None
    operating_income: Optional[float] = None
    net_income: Optional[float] = None
    ebitda: Optional[float] = None
    ebit: Optional[float] = None
    eps: Optional[float] = None
    revenue_growth: Optional[float] = None
    net_income_growth: Optional[float] = None
    eps_growth: Optional[float] = None
    
    # Margins
    gross_margin: Optional[float] = None
    operating_margin: Optional[float] = None
    profit_margin: Optional[float] = None
    fcf_margin: Optional[float] = None
    
    # Cash Flow
    operating_cash_flow: Optional[float] = None
    investing_cash_flow: Optional[float] = None
    financing_cash_flow: Optional[float] = None
    net_cash_flow: Optional[float] = None
    capital_expenditures: Optional[float] = None
    free_cash_flow: Optional[float] = None
    
    # Balance Sheet
    total_cash: Optional[float] = None
    total_debt: Optional[float] = None
    net_cash_debt: Optional[float] = None
    total_assets: Optional[float] = None
    total_liabilities: Optional[float] = None
    shareholders_equity: Optional[float] = None
    working_capital: Optional[float] = None
    book_value_per_share: Optional[float] = None
    shares_outstanding: Optional[int] = None
    
    # Ratios
    roe: Optional[float] = None
    roa: Optional[float] = None
    roic: Optional[float] = None
    roce: Optional[float] = None
    current_ratio: Optional[float] = None
    quick_ratio: Optional[float] = None
    debt_equity: Optional[float] = None
    debt_ebitda: Optional[float] = None
    interest_coverage: Optional[float] = None
    altman_z_score: Optional[float] = None
    piotroski_f_score: Optional[int] = None
    
    # Technicals
    rsi: Optional[float] = None
    beta: Optional[float] = None
    ma_20: Optional[float] = None
    ma_50: Optional[float] = None
    ma_200: Optional[float] = None
    
    # Dividends
    dividend_yield: Optional[float] = None
    dividend_per_share: Optional[float] = None
    ex_div_date: Optional[str] = None

    class Config:
        orm_mode = True

class StockComparisonBrief(BaseModel):
    id: int
    symbol: str
    sector: Optional[str] = None
    rank: Optional[int] = None

    class Config:
        orm_mode = True

class PopularComparisonResponse(BaseModel):
    stocks: List[StockComparisonBrief]

    class Config:
        orm_mode = True
