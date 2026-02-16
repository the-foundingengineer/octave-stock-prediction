from datetime import datetime, date
from decimal import Decimal
from typing import Optional
from sqlalchemy import Column, Integer, String, ForeignKey, BigInteger, Float, UniqueConstraint
from sqlalchemy.orm import relationship
from app.database import Base



from dotenv import load_dotenv
from sqlalchemy import (
    create_engine, Column, Integer, BigInteger, String,
    Numeric, Date, DateTime, ForeignKey, UniqueConstraint, Text
)
from sqlalchemy.orm import DeclarativeBase, relationship, Session

load_dotenv()


class Stock(Base):
    """
    Static company profile. One row per ticker.
    All financial tables reference this via stock_id (FK).
    """
    __tablename__ = "stocks"

    id              = Column(Integer, primary_key=True, index=True)
    symbol          = Column(String(20), unique=True, index=True, nullable=False)
    name            = Column(String(200), nullable=True)
    exchange        = Column(String(50), nullable=True)   # e.g. "Nigerian Stock Exchange"
    currency        = Column(String(10), nullable=True)   # e.g. "NGN"
    sector          = Column(String(100), nullable=True)
    industry        = Column(String(100), nullable=True)
    description     = Column(Text, nullable=True)
    website         = Column(String(200), nullable=True)
    country         = Column(String(100), nullable=True)
    founded         = Column(String(10), nullable=True)   # year as string
    ceo             = Column(String(100), nullable=True)
    employees       = Column(Integer, nullable=True)
    fiscal_year_end = Column(String(20), nullable=True)   # e.g. "March 31"
    sic_code        = Column(String(10), nullable=True)
    reporting_currency = Column(String(10), nullable=True)  # financials currency (often USD)
    last_updated    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    income_stmts    = relationship("IncomeStatement", back_populates="stock", cascade="all, delete-orphan")
    balance_sheets  = relationship("BalanceSheet", back_populates="stock", cascade="all, delete-orphan")
    cash_flows      = relationship("CashFlow", back_populates="stock", cascade="all, delete-orphan")
    ratios          = relationship("StockRatio", back_populates="stock", cascade="all, delete-orphan")
    daily_klines    = relationship("DailyKline", back_populates="stock")



class DailyKline(Base):
    __tablename__ = "daily_klines"

    id        = Column(Integer, primary_key=True, index=True)
    symbol    = Column(String, ForeignKey("stocks.symbol"), index=True)
    date      = Column(String, index=True)            # YYYY-MM-DD  ← keep as-is
    timestamp = Column(BigInteger, nullable=True)     # epoch ms from API

    # ── OHLCV (existing) ──────────────────────────────────────────────────────
    open     = Column(Float, nullable=True)
    high     = Column(Float, nullable=True)
    low      = Column(Float, nullable=True)
    close    = Column(Float, nullable=True)
    volume   = Column(BigInteger, nullable=True)
    turnover = Column(Float, nullable=True)

    # ── 52-week range (new) ───────────────────────────────────────────────────
    week_52_high = Column(Float, nullable=True)
    week_52_low  = Column(Float, nullable=True)

    # ── Technicals (new) ──────────────────────────────────────────────────────
    avg_volume_20d = Column(BigInteger, nullable=True)
    rsi            = Column(Float, nullable=True)
    ma_50d         = Column(Float, nullable=True)
    ma_200d        = Column(Float, nullable=True)
    beta           = Column(Float, nullable=True)

    # ── Market valuation snapshot (new) ───────────────────────────────────────
    market_cap       = Column(Numeric(28, 2), nullable=True)
    enterprise_value = Column(Numeric(28, 2), nullable=True)
    pe_ratio         = Column(Float, nullable=True)
    forward_pe       = Column(Float, nullable=True)
    ps_ratio         = Column(Float, nullable=True)
    pb_ratio         = Column(Float, nullable=True)

    # ── Dividends (new) ───────────────────────────────────────────────────────
    dividend_per_share = Column(Float, nullable=True)
    dividend_yield     = Column(Float, nullable=True)
    ex_dividend_date   = Column(String, nullable=True)   # YYYY-MM-DD string, matches your date style

    # ── Corporate actions (new) ───────────────────────────────────────────────
    adjustment_factor = Column(String, nullable=True)    # "v" from stock/split

    stock = relationship("Stock", back_populates="daily_klines")

    __table_args__ = (
        UniqueConstraint("symbol", "date", name="uix_symbol_date"),
    )


class IncomeStatement(Base):
    """
    One row per (stock, fiscal period). Stores TTM and each FY.
    period_type: 'TTM' | 'FY' | 'Q'
    """
    __tablename__ = "income_statements"
    __table_args__ = (UniqueConstraint("stock_id", "period_ending", "period_type", name="uq_income_period"),)

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    period_ending   = Column(Date, nullable=False)
    period_type     = Column(String(10), nullable=False)   # 'TTM', 'FY', 'Q1' …

    # Revenue
    revenue         = Column(Numeric(24, 2), nullable=True)
    operating_revenue = Column(Numeric(24, 2), nullable=True)
    other_revenue   = Column(Numeric(24, 2), nullable=True)
    revenue_growth_yoy = Column(Numeric(10, 4), nullable=True)  # as decimal, e.g. 0.2588

    # Costs
    cost_of_revenue = Column(Numeric(24, 2), nullable=True)
    gross_profit    = Column(Numeric(24, 2), nullable=True)
    sga_expenses    = Column(Numeric(24, 2), nullable=True)
    other_opex      = Column(Numeric(24, 2), nullable=True)
    total_opex      = Column(Numeric(24, 2), nullable=True)

    # Income
    operating_income = Column(Numeric(24, 2), nullable=True)
    ebitda          = Column(Numeric(24, 2), nullable=True)
    ebit            = Column(Numeric(24, 2), nullable=True)
    interest_expense = Column(Numeric(24, 2), nullable=True)
    pretax_income   = Column(Numeric(24, 2), nullable=True)
    income_tax      = Column(Numeric(24, 2), nullable=True)
    net_income      = Column(Numeric(24, 2), nullable=True)
    net_income_growth_yoy = Column(Numeric(10, 4), nullable=True)
    minority_interest = Column(Numeric(24, 2), nullable=True)

    # Per-share
    eps_basic       = Column(Numeric(14, 4), nullable=True)
    eps_diluted     = Column(Numeric(14, 4), nullable=True)
    eps_growth_yoy  = Column(Numeric(10, 4), nullable=True)
    dividend_per_share = Column(Numeric(14, 4), nullable=True)
    dividend_growth_yoy = Column(Numeric(10, 4), nullable=True)

    # Shares
    shares_basic    = Column(BigInteger, nullable=True)
    shares_diluted  = Column(BigInteger, nullable=True)
    shares_change_yoy = Column(Numeric(10, 4), nullable=True)

    # Margins
    gross_margin    = Column(Numeric(10, 4), nullable=True)
    operating_margin = Column(Numeric(10, 4), nullable=True)
    profit_margin   = Column(Numeric(10, 4), nullable=True)
    ebitda_margin   = Column(Numeric(10, 4), nullable=True)
    effective_tax_rate = Column(Numeric(10, 4), nullable=True)

    # Cash generation (placed here for convenience)
    free_cash_flow  = Column(Numeric(24, 2), nullable=True)
    fcf_per_share   = Column(Numeric(14, 4), nullable=True)
    fcf_margin      = Column(Numeric(10, 4), nullable=True)

    stock = relationship("Stock", back_populates="income_stmts")


class BalanceSheet(Base):
    """One row per (stock, fiscal period)."""
    __tablename__ = "balance_sheets"
    __table_args__ = (UniqueConstraint("stock_id", "period_ending", "period_type", name="uq_balance_period"),)

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    period_ending   = Column(Date, nullable=False)
    period_type     = Column(String(10), nullable=False)

    # Current assets
    cash_equivalents = Column(Numeric(24, 2), nullable=True)
    short_term_investments = Column(Numeric(24, 2), nullable=True)
    cash_and_st_investments = Column(Numeric(24, 2), nullable=True)
    accounts_receivable = Column(Numeric(24, 2), nullable=True)
    inventory       = Column(Numeric(24, 2), nullable=True)
    restricted_cash = Column(Numeric(24, 2), nullable=True)
    other_current_assets = Column(Numeric(24, 2), nullable=True)
    total_current_assets = Column(Numeric(24, 2), nullable=True)

    # Non-current assets
    ppe             = Column(Numeric(24, 2), nullable=True)   # Property Plant & Equipment
    goodwill        = Column(Numeric(24, 2), nullable=True)
    intangible_assets = Column(Numeric(24, 2), nullable=True)
    long_term_investments = Column(Numeric(24, 2), nullable=True)
    total_assets    = Column(Numeric(24, 2), nullable=True)

    # Current liabilities
    accounts_payable = Column(Numeric(24, 2), nullable=True)
    short_term_debt = Column(Numeric(24, 2), nullable=True)
    current_ltdebt  = Column(Numeric(24, 2), nullable=True)
    current_leases  = Column(Numeric(24, 2), nullable=True)
    unearned_revenue_current = Column(Numeric(24, 2), nullable=True)
    total_current_liabilities = Column(Numeric(24, 2), nullable=True)

    # Non-current liabilities
    long_term_debt  = Column(Numeric(24, 2), nullable=True)
    long_term_leases = Column(Numeric(24, 2), nullable=True)
    total_liabilities = Column(Numeric(24, 2), nullable=True)

    # Equity
    common_stock    = Column(Numeric(24, 2), nullable=True)
    retained_earnings = Column(Numeric(24, 2), nullable=True)
    total_common_equity = Column(Numeric(24, 2), nullable=True)
    minority_interest = Column(Numeric(24, 2), nullable=True)
    shareholders_equity = Column(Numeric(24, 2), nullable=True)

    # Derived
    total_debt      = Column(Numeric(24, 2), nullable=True)
    net_cash_debt   = Column(Numeric(24, 2), nullable=True)
    net_cash_per_share = Column(Numeric(14, 4), nullable=True)
    working_capital = Column(Numeric(24, 2), nullable=True)
    book_value_per_share = Column(Numeric(14, 4), nullable=True)
    tangible_book_value = Column(Numeric(24, 2), nullable=True)
    tangible_bvps   = Column(Numeric(14, 4), nullable=True)
    shares_outstanding = Column(BigInteger, nullable=True)

    stock = relationship("Stock", back_populates="balance_sheets")


class CashFlow(Base):
    """One row per (stock, fiscal period)."""
    __tablename__ = "cash_flows"
    __table_args__ = (UniqueConstraint("stock_id", "period_ending", "period_type", name="uq_cashflow_period"),)

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    period_ending   = Column(Date, nullable=False)
    period_type     = Column(String(10), nullable=False)

    # Operating
    net_income      = Column(Numeric(24, 2), nullable=True)
    depreciation_amortization = Column(Numeric(24, 2), nullable=True)
    operating_cash_flow = Column(Numeric(24, 2), nullable=True)
    ocf_growth_yoy  = Column(Numeric(10, 4), nullable=True)

    # Investing
    capex           = Column(Numeric(24, 2), nullable=True)
    sale_purchase_intangibles = Column(Numeric(24, 2), nullable=True)
    investing_cash_flow = Column(Numeric(24, 2), nullable=True)

    # Financing
    debt_issued     = Column(Numeric(24, 2), nullable=True)
    debt_repaid     = Column(Numeric(24, 2), nullable=True)
    net_debt_change = Column(Numeric(24, 2), nullable=True)
    buybacks        = Column(Numeric(24, 2), nullable=True)
    dividends_paid  = Column(Numeric(24, 2), nullable=True)
    financing_cash_flow = Column(Numeric(24, 2), nullable=True)

    # Totals
    net_cash_flow   = Column(Numeric(24, 2), nullable=True)
    free_cash_flow  = Column(Numeric(24, 2), nullable=True)
    fcf_growth_yoy  = Column(Numeric(10, 4), nullable=True)
    fcf_margin      = Column(Numeric(10, 4), nullable=True)
    fcf_per_share   = Column(Numeric(14, 4), nullable=True)
    levered_fcf     = Column(Numeric(24, 2), nullable=True)
    unlevered_fcf   = Column(Numeric(24, 2), nullable=True)

    # Supplemental
    cash_interest_paid = Column(Numeric(24, 2), nullable=True)
    cash_tax_paid   = Column(Numeric(24, 2), nullable=True)
    change_in_working_capital = Column(Numeric(24, 2), nullable=True)

    stock = relationship("Stock", back_populates="cash_flows")


class StockRatio(Base):
    """
    Valuation, profitability, leverage, and efficiency ratios.
    One row per (stock, period). 'current' period_type holds live ratios.
    """
    __tablename__ = "stock_ratios"
    __table_args__ = (UniqueConstraint("stock_id", "period_ending", "period_type", name="uq_ratio_period"),)

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    period_ending   = Column(Date, nullable=False)
    period_type     = Column(String(10), nullable=False)   # 'current', 'FY', …

    # Valuation
    pe_ratio        = Column(Numeric(12, 4), nullable=True)
    ps_ratio        = Column(Numeric(12, 4), nullable=True)
    pb_ratio        = Column(Numeric(12, 4), nullable=True)
    p_fcf_ratio     = Column(Numeric(12, 4), nullable=True)
    p_ocf_ratio     = Column(Numeric(12, 4), nullable=True)
    ev_sales        = Column(Numeric(12, 4), nullable=True)
    ev_ebitda       = Column(Numeric(12, 4), nullable=True)
    ev_ebit         = Column(Numeric(12, 4), nullable=True)
    ev_fcf          = Column(Numeric(12, 4), nullable=True)
    market_cap      = Column(Numeric(28, 2), nullable=True)
    enterprise_value = Column(Numeric(28, 2), nullable=True)
    market_cap_growth_yoy = Column(Numeric(10, 4), nullable=True)
    last_close_price = Column(Numeric(18, 4), nullable=True)

    # Leverage
    debt_equity     = Column(Numeric(12, 4), nullable=True)
    debt_ebitda     = Column(Numeric(12, 4), nullable=True)
    debt_fcf        = Column(Numeric(12, 4), nullable=True)
    interest_coverage = Column(Numeric(12, 4), nullable=True)

    # Liquidity
    current_ratio   = Column(Numeric(12, 4), nullable=True)
    quick_ratio     = Column(Numeric(12, 4), nullable=True)

    # Efficiency
    asset_turnover  = Column(Numeric(12, 4), nullable=True)
    inventory_turnover = Column(Numeric(12, 4), nullable=True)

    # Returns
    roe             = Column(Numeric(10, 4), nullable=True)
    roa             = Column(Numeric(10, 4), nullable=True)
    roic            = Column(Numeric(10, 4), nullable=True)
    roce            = Column(Numeric(10, 4), nullable=True)

    # Yield & Shareholder returns
    earnings_yield  = Column(Numeric(10, 4), nullable=True)
    fcf_yield       = Column(Numeric(10, 4), nullable=True)
    dividend_yield  = Column(Numeric(10, 4), nullable=True)
    buyback_yield   = Column(Numeric(10, 4), nullable=True)
    total_shareholder_return = Column(Numeric(10, 4), nullable=True)
    payout_ratio    = Column(Numeric(10, 4), nullable=True)

    # Risk/quality scores
    altman_z_score  = Column(Numeric(8, 4), nullable=True)
    piotroski_f_score = Column(Integer, nullable=True)
    beta            = Column(Numeric(8, 4), nullable=True)

    stock = relationship("Stock", back_populates="ratios")


# Create all tables
def init_db():
    Base.metadata.create_all(bind=engine)

