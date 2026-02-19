"""
Pydantic response/request schemas for the Octave stock API.

Organized by domain:
    - Stock basics (list, detail, create)
    - Kline / chart data
    - Stats, info, related stocks
    - Income statement
    - Search
    - Stock comparison (single & bulk)
"""

from datetime import date
from pydantic import BaseModel
from typing import List, Optional


# ── Stock Basics ─────────────────────────────────────────────────────────────


class StockRecordBase(BaseModel):
    """Base fields for a daily OHLCV record."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: Optional[int] = None
    symbol: str


class StockRecordCreate(StockRecordBase):
    """Request body for creating a new stock record."""
    pass


class StockRecord(StockRecordBase):
    """Response for a single stock record (includes DB id)."""
    id: int

    class Config:
        from_attributes = True


class Stock(BaseModel):
    """Lightweight stock profile returned in list endpoints."""
    id: int
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    description: Optional[str] = None
    website: Optional[str] = None
    currency: Optional[str] = None
    stock_exchange: Optional[str] = None
    last_updated: Optional[str] = None

    class Config:
        from_attributes = True


# ── Kline / Chart Data ──────────────────────────────────────────────────────


class KlineData(BaseModel):
    """Single OHLCV candle."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: float


class KlineResponse(BaseModel):
    """Response wrapper for a list of klines."""
    stock_id: int
    symbol: str
    interval: str
    klines: List[KlineData]

    class Config:
        from_attributes = True


# ── Stock Stats ──────────────────────────────────────────────────────────────


class StockStatsResponse(BaseModel):
    """Aggregated statistics for a single stock."""
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
    payout_ratio: Optional[float] = None
    dividend_growth: Optional[float] = None
    payout_frequency: Optional[str] = None
    revenue_growth: Optional[float] = None
    revenue_per_employee: Optional[float] = None

    class Config:
        from_attributes = True


# ── Stock Info ───────────────────────────────────────────────────────────────


class StockInfoResponse(BaseModel):
    """Lightweight profile + technical info for a stock."""
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
        from_attributes = True


# ── Related Stocks ───────────────────────────────────────────────────────────


class StockRelatedResponse(BaseModel):
    """Summary of a related stock (same sector)."""
    stock_id: int
    symbol: str
    market_cap: Optional[float] = None
    revenue_ttm: Optional[float] = None

    class Config:
        from_attributes = True


# ── Income Statement ────────────────────────────────────────────────────────


class IncomeStatementResponse(BaseModel):
    """Single income statement period."""
    id: int
    stock_id: int
    period_ending: date
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
        from_attributes = True


class StockWithIncomeStatementResponse(BaseModel):
    """Stock profile bundled with its latest income statement."""
    id: int
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    stock_exchange: Optional[str] = None
    currency: Optional[str] = None
    country: Optional[str] = None
    website: Optional[str] = None
    ceo: Optional[str] = None
    employees: Optional[int] = None
    fiscal_year_end: Optional[str] = None
    headquarters: Optional[str] = None
    income_statement: Optional[IncomeStatementResponse] = None

    class Config:
        from_attributes = True


# ── Search ───────────────────────────────────────────────────────────────────


class StockSearchResult(BaseModel):
    """Single result from the stock search endpoint."""
    id: int
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None

    class Config:
        from_attributes = True


# ── Stock Comparison ─────────────────────────────────────────────────────────


class StockComparisonBrief(BaseModel):
    """Brief stock info used in the popular-comparisons list."""
    id: int
    symbol: str
    sector: Optional[str] = None
    rank: Optional[int] = None

    class Config:
        from_attributes = True


class PopularComparisonResponse(BaseModel):
    """Wrapper for the popular comparisons endpoint."""
    stocks: List[StockComparisonBrief]

    class Config:
        from_attributes = True


class StockComparisonItem(BaseModel):
    """
    Full comparison data for a single stock.
    Covers price, valuation, financials, margins, cash flow,
    balance sheet, ratios, technicals, and dividends.
    """
    # Basic info
    symbol: str
    name: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    stock_exchange: Optional[str] = None
    website: Optional[str] = None
    country: Optional[str] = None
    employees: Optional[int] = None
    founded: Optional[str] = None
    ipo_date: Optional[str] = None

    # Price data (latest)
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

    # 52-week
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

    # Financials (TTM / recent)
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

    # Cash flow
    operating_cash_flow: Optional[float] = None
    investing_cash_flow: Optional[float] = None
    financing_cash_flow: Optional[float] = None
    net_cash_flow: Optional[float] = None
    capital_expenditures: Optional[float] = None
    free_cash_flow: Optional[float] = None

    # Balance sheet
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
    payout_ratio: Optional[float] = None
    dividend_growth: Optional[float] = None
    payout_frequency: Optional[str] = None
    revenue_ttm: Optional[float] = None
    revenue_growth: Optional[float] = None
    revenue_per_employee: Optional[float] = None

    class Config:
        from_attributes = True


# ── Bulk Comparison ──────────────────────────────────────────────────────────


class BulkComparisonItem(BaseModel):
    """Kline + stats bundle for one stock in a bulk comparison."""
    stock_id: int
    symbol: str
    klines: List[KlineData]
    stats: Optional[StockStatsResponse] = None

    class Config:
        from_attributes = True


class BulkComparisonResponse(BaseModel):
    """Response wrapper for the bulk compare endpoint."""
    comparisons: List[BulkComparisonItem]

    class Config:
        from_attributes = True


# ── Dividends ────────────────────────────────────────────────────────────────


class DividendResponse(BaseModel):
    """Single dividend payout record."""
    id: int
    stock_id: int
    ex_dividend_date: str
    record_date: Optional[str] = None
    pay_date: Optional[str] = None
    amount: float
    currency: Optional[str] = None
    frequency: Optional[str] = None

    class Config:
        from_attributes = True





# ── Market Cap History ───────────────────────────────────────────────────────


class MarketCapHistoryItem(BaseModel):
    """Single market cap history record."""
    id: int
    stock_id: int
    date: str
    market_cap: Optional[float] = None
    frequency: Optional[str] = None

    class Config:
        from_attributes = True


class MarketCapHistoryResponse(BaseModel):
    """Response wrapper for market cap history endpoint."""
    stock_id: int
    symbol: str
    history: List[MarketCapHistoryItem]

    class Config:
        from_attributes = True


# ── Stock Profile & Executives ──────────────────────────────────────────────


class StockExecutiveResponse(BaseModel):
    """Management team member."""
    id: int
    name: str
    title: Optional[str] = None
    age: Optional[int] = None
    since: Optional[str] = None

    class Config:
        from_attributes = True


class StockProfileResponse(BaseModel):
    """Detailed company profile."""
    id: int
    symbol: str
    name: Optional[str] = None
    description: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None
    exchange: Optional[str] = None
    currency: Optional[str] = None
    country: Optional[str] = None
    founded: Optional[str] = None
    headquarters: Optional[str] = None
    website: Optional[str] = None
    employees: Optional[int] = None
    ceo: Optional[str] = None
    executives: List["StockExecutiveResponse"] = []

    class Config:
        from_attributes = True
