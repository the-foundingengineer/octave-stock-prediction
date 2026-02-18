"""
SQLAlchemy ORM models for the Octave stock prediction API.

Tables:
    - stocks           : Static company profile (one row per ticker)
    - daily_klines     : Daily OHLCV + technicals
    - income_statements: Income statement by fiscal period
    - balance_sheets   : Balance sheet by fiscal period
    - cash_flows       : Cash flow statement by fiscal period
    - stock_ratios     : Valuation, profitability & leverage ratios
    - dividends        : Historical dividend distributions
    - stock_executives : Company management team
    - market_cap_history: Historical market capitalization
"""

from datetime import datetime

from sqlalchemy import (
    BigInteger, Column, Date, DateTime, Float, Integer,
    Numeric, String, Text, ForeignKey, UniqueConstraint,
)
from sqlalchemy.orm import relationship

from app.database import Base


# ── Company Profile ──────────────────────────────────────────────────────────


class Stock(Base):
    """
    Static company profile. One row per ticker.
    All financial tables reference this via stock_id (FK).
    """
    __tablename__ = "stocks"

    id              = Column(Integer, primary_key=True, index=True)
    symbol          = Column(String(20), unique=True, index=True, nullable=False)
    name            = Column(String(200), nullable=True)
    exchange        = Column(String(50), nullable=True)
    currency        = Column(String(10), nullable=True)
    sector          = Column(String(100), nullable=True)
    industry        = Column(String(100), nullable=True)
    description     = Column(Text, nullable=True)
    website         = Column(String(200), nullable=True)
    headquarters    = Column(String(200), nullable=True)
    country         = Column(String(100), nullable=True)
    founded         = Column(String(10), nullable=True)
    ceo             = Column(String(100), nullable=True)
    employees       = Column(Integer, nullable=True)
    fiscal_year_end = Column(String(20), nullable=True)
    sic_code        = Column(String(10), nullable=True)
    reporting_currency = Column(String(10), nullable=True)
    last_updated    = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    # Relationships
    income_stmts   = relationship("IncomeStatement", back_populates="stock", cascade="all, delete-orphan")
    balance_sheets = relationship("BalanceSheet",     back_populates="stock", cascade="all, delete-orphan")
    cash_flows     = relationship("CashFlow",         back_populates="stock", cascade="all, delete-orphan")
    ratios         = relationship("StockRatio",       back_populates="stock", cascade="all, delete-orphan")
    daily_klines   = relationship("DailyKline",       back_populates="stock")
    dividends      = relationship("Dividend",         back_populates="stock", cascade="all, delete-orphan")
    executives     = relationship("StockExecutive",   back_populates="stock", cascade="all, delete-orphan")
    market_cap_history = relationship("MarketCapHistory", back_populates="stock", cascade="all, delete-orphan")


# ── Daily Market Data ────────────────────────────────────────────────────────


class DailyKline(Base):
    """
    Daily OHLCV candle with technical indicators.

    Valuation, dividend, and revenue snapshot columns have been moved
    to their dedicated tables (stock_ratios, dividends, income_statements).
    """
    __tablename__ = "daily_klines"

    id        = Column(Integer, primary_key=True, index=True)
    stock_id  = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    date      = Column(String, index=True)
    timestamp = Column(BigInteger, nullable=True)

    # OHLCV
    open     = Column(Float, nullable=True)
    high     = Column(Float, nullable=True)
    low      = Column(Float, nullable=True)
    close    = Column(Float, nullable=True)
    volume   = Column(BigInteger, nullable=True)
    turnover = Column(Float, nullable=True)

    # 52-week range
    week_52_high = Column(Float, nullable=True)
    week_52_low  = Column(Float, nullable=True)

    # Technicals
    avg_volume_20d = Column(BigInteger, nullable=True)
    rsi            = Column(Float, nullable=True)
    ma_50d         = Column(Float, nullable=True)
    ma_200d        = Column(Float, nullable=True)
    beta           = Column(Float, nullable=True)

    # Corporate actions
    adjustment_factor = Column(String, nullable=True)

    stock = relationship("Stock", back_populates="daily_klines")

    __table_args__ = (
        UniqueConstraint("stock_id", "date", name="uix_stock_id_date"),
    )


# ── Income Statement ────────────────────────────────────────────────────────


class IncomeStatement(Base):
    """
    One row per (stock, fiscal period).
    period_type: 'TTM' | 'FY' | 'Q1' …
    """
    __tablename__ = "income_statements"
    __table_args__ = (UniqueConstraint("stock_id", "period_ending", "period_type", name="uq_income_period"),)

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    period_ending   = Column(Date, nullable=False)
    period_type     = Column(String(10), nullable=False)

    # Revenue
    revenue            = Column(Numeric(24, 2), nullable=True)
    operating_revenue  = Column(Numeric(24, 2), nullable=True)
    other_revenue      = Column(Numeric(24, 2), nullable=True)
    revenue_growth_yoy = Column(Numeric(10, 4), nullable=True)

    # Costs
    cost_of_revenue = Column(Numeric(24, 2), nullable=True)
    gross_profit    = Column(Numeric(24, 2), nullable=True)
    sga_expenses    = Column(Numeric(24, 2), nullable=True)
    other_opex      = Column(Numeric(24, 2), nullable=True)
    total_opex      = Column(Numeric(24, 2), nullable=True)

    # Income
    operating_income      = Column(Numeric(24, 2), nullable=True)
    ebitda                = Column(Numeric(24, 2), nullable=True)
    ebit                  = Column(Numeric(24, 2), nullable=True)
    interest_expense      = Column(Numeric(24, 2), nullable=True)
    pretax_income         = Column(Numeric(24, 2), nullable=True)
    income_tax            = Column(Numeric(24, 2), nullable=True)
    net_income            = Column(Numeric(24, 2), nullable=True)
    net_income_growth_yoy = Column(Numeric(10, 4), nullable=True)
    minority_interest     = Column(Numeric(24, 2), nullable=True)

    # Per-share
    eps_basic           = Column(Numeric(14, 4), nullable=True)
    eps_diluted         = Column(Numeric(14, 4), nullable=True)
    eps_growth_yoy      = Column(Numeric(10, 4), nullable=True)
    dividend_per_share  = Column(Numeric(14, 4), nullable=True)
    dividend_growth_yoy = Column(Numeric(10, 4), nullable=True)

    # Shares
    shares_basic      = Column(BigInteger, nullable=True)
    shares_diluted    = Column(BigInteger, nullable=True)
    shares_change_yoy = Column(Numeric(10, 4), nullable=True)

    # Margins
    gross_margin       = Column(Numeric(10, 4), nullable=True)
    operating_margin   = Column(Numeric(10, 4), nullable=True)
    profit_margin      = Column(Numeric(10, 4), nullable=True)
    ebitda_margin      = Column(Numeric(10, 4), nullable=True)
    effective_tax_rate = Column(Numeric(10, 4), nullable=True)

    # Cash generation
    free_cash_flow = Column(Numeric(24, 2), nullable=True)
    fcf_per_share  = Column(Numeric(14, 4), nullable=True)
    fcf_margin     = Column(Numeric(10, 4), nullable=True)

    stock = relationship("Stock", back_populates="income_stmts")


# ── Balance Sheet ────────────────────────────────────────────────────────────


class BalanceSheet(Base):
    """One row per (stock, fiscal period)."""
    __tablename__ = "balance_sheets"
    __table_args__ = (UniqueConstraint("stock_id", "period_ending", "period_type", name="uq_balance_period"),)

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    period_ending   = Column(Date, nullable=False)
    period_type     = Column(String(10), nullable=False)

    # Current assets
    cash_equivalents        = Column(Numeric(24, 2), nullable=True)
    short_term_investments  = Column(Numeric(24, 2), nullable=True)
    cash_and_st_investments = Column(Numeric(24, 2), nullable=True)
    accounts_receivable     = Column(Numeric(24, 2), nullable=True)
    inventory               = Column(Numeric(24, 2), nullable=True)
    restricted_cash         = Column(Numeric(24, 2), nullable=True)
    other_current_assets    = Column(Numeric(24, 2), nullable=True)
    total_current_assets    = Column(Numeric(24, 2), nullable=True)

    # Non-current assets
    ppe                  = Column(Numeric(24, 2), nullable=True)
    goodwill             = Column(Numeric(24, 2), nullable=True)
    intangible_assets    = Column(Numeric(24, 2), nullable=True)
    long_term_investments = Column(Numeric(24, 2), nullable=True)
    total_assets         = Column(Numeric(24, 2), nullable=True)

    # Current liabilities
    accounts_payable          = Column(Numeric(24, 2), nullable=True)
    short_term_debt           = Column(Numeric(24, 2), nullable=True)
    current_ltdebt            = Column(Numeric(24, 2), nullable=True)
    current_leases            = Column(Numeric(24, 2), nullable=True)
    unearned_revenue_current  = Column(Numeric(24, 2), nullable=True)
    total_current_liabilities = Column(Numeric(24, 2), nullable=True)

    # Non-current liabilities
    long_term_debt     = Column(Numeric(24, 2), nullable=True)
    long_term_leases   = Column(Numeric(24, 2), nullable=True)
    total_liabilities  = Column(Numeric(24, 2), nullable=True)

    # Equity
    common_stock        = Column(Numeric(24, 2), nullable=True)
    retained_earnings   = Column(Numeric(24, 2), nullable=True)
    total_common_equity = Column(Numeric(24, 2), nullable=True)
    minority_interest   = Column(Numeric(24, 2), nullable=True)
    shareholders_equity = Column(Numeric(24, 2), nullable=True)

    # Derived
    total_debt           = Column(Numeric(24, 2), nullable=True)
    net_cash_debt        = Column(Numeric(24, 2), nullable=True)
    net_cash_per_share   = Column(Numeric(14, 4), nullable=True)
    working_capital      = Column(Numeric(24, 2), nullable=True)
    book_value_per_share = Column(Numeric(14, 4), nullable=True)
    tangible_book_value  = Column(Numeric(24, 2), nullable=True)
    tangible_bvps        = Column(Numeric(14, 4), nullable=True)
    shares_outstanding   = Column(BigInteger, nullable=True)

    stock = relationship("Stock", back_populates="balance_sheets")


# ── Cash Flow Statement ─────────────────────────────────────────────────────


class CashFlow(Base):
    """One row per (stock, fiscal period)."""
    __tablename__ = "cash_flows"
    __table_args__ = (UniqueConstraint("stock_id", "period_ending", "period_type", name="uq_cashflow_period"),)

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    period_ending   = Column(Date, nullable=False)
    period_type     = Column(String(10), nullable=False)

    # Operating
    net_income                = Column(Numeric(24, 2), nullable=True)
    depreciation_amortization = Column(Numeric(24, 2), nullable=True)
    operating_cash_flow       = Column(Numeric(24, 2), nullable=True)
    ocf_growth_yoy            = Column(Numeric(10, 4), nullable=True)

    # Investing
    capex                      = Column(Numeric(24, 2), nullable=True)
    sale_purchase_intangibles  = Column(Numeric(24, 2), nullable=True)
    investing_cash_flow        = Column(Numeric(24, 2), nullable=True)

    # Financing
    debt_issued         = Column(Numeric(24, 2), nullable=True)
    debt_repaid         = Column(Numeric(24, 2), nullable=True)
    net_debt_change     = Column(Numeric(24, 2), nullable=True)
    buybacks            = Column(Numeric(24, 2), nullable=True)
    dividends_paid      = Column(Numeric(24, 2), nullable=True)
    financing_cash_flow = Column(Numeric(24, 2), nullable=True)

    # Totals
    net_cash_flow  = Column(Numeric(24, 2), nullable=True)
    free_cash_flow = Column(Numeric(24, 2), nullable=True)
    fcf_growth_yoy = Column(Numeric(10, 4), nullable=True)
    fcf_margin     = Column(Numeric(10, 4), nullable=True)
    fcf_per_share  = Column(Numeric(14, 4), nullable=True)
    levered_fcf    = Column(Numeric(24, 2), nullable=True)
    unlevered_fcf  = Column(Numeric(24, 2), nullable=True)

    # Supplemental
    cash_interest_paid        = Column(Numeric(24, 2), nullable=True)
    cash_tax_paid             = Column(Numeric(24, 2), nullable=True)
    change_in_working_capital = Column(Numeric(24, 2), nullable=True)

    stock = relationship("Stock", back_populates="cash_flows")


# ── Financial Ratios ─────────────────────────────────────────────────────────


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
    period_type     = Column(String(10), nullable=False)

    # Valuation
    pe_ratio              = Column(Numeric(12, 4), nullable=True)
    ps_ratio              = Column(Numeric(12, 4), nullable=True)
    pb_ratio              = Column(Numeric(12, 4), nullable=True)
    p_fcf_ratio           = Column(Numeric(12, 4), nullable=True)
    p_ocf_ratio           = Column(Numeric(12, 4), nullable=True)
    ev_sales              = Column(Numeric(12, 4), nullable=True)
    ev_ebitda             = Column(Numeric(12, 4), nullable=True)
    ev_ebit               = Column(Numeric(12, 4), nullable=True)
    ev_fcf                = Column(Numeric(12, 4), nullable=True)
    market_cap            = Column(Numeric(28, 2), nullable=True)
    enterprise_value      = Column(Numeric(28, 2), nullable=True)
    market_cap_growth_yoy = Column(Numeric(10, 4), nullable=True)
    last_close_price      = Column(Numeric(18, 4), nullable=True)

    # Leverage
    debt_equity       = Column(Numeric(12, 4), nullable=True)
    debt_ebitda       = Column(Numeric(12, 4), nullable=True)
    debt_fcf          = Column(Numeric(12, 4), nullable=True)
    interest_coverage = Column(Numeric(12, 4), nullable=True)

    # Liquidity
    current_ratio = Column(Numeric(12, 4), nullable=True)
    quick_ratio   = Column(Numeric(12, 4), nullable=True)

    # Efficiency
    asset_turnover     = Column(Numeric(12, 4), nullable=True)
    inventory_turnover = Column(Numeric(12, 4), nullable=True)

    # Returns
    roe  = Column(Numeric(10, 4), nullable=True)
    roa  = Column(Numeric(10, 4), nullable=True)
    roic = Column(Numeric(10, 4), nullable=True)
    roce = Column(Numeric(10, 4), nullable=True)

    # Yield & shareholder returns
    earnings_yield           = Column(Numeric(10, 4), nullable=True)
    fcf_yield                = Column(Numeric(10, 4), nullable=True)
    dividend_yield           = Column(Numeric(10, 4), nullable=True)
    buyback_yield            = Column(Numeric(10, 4), nullable=True)
    total_shareholder_return = Column(Numeric(10, 4), nullable=True)
    payout_ratio             = Column(Numeric(10, 4), nullable=True)

    # Risk / quality scores
    altman_z_score    = Column(Numeric(8, 4), nullable=True)
    piotroski_f_score = Column(Integer, nullable=True)
    beta              = Column(Numeric(8, 4), nullable=True)

    stock = relationship("Stock", back_populates="ratios")


# ── Dividends ────────────────────────────────────────────────────────────────


class Dividend(Base):
    """
    Historical dividend distributions for a stock.
    One row per dividend event.
    """
    __tablename__ = "dividends"

    id              = Column(Integer, primary_key=True, index=True)
    stock_id        = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    ex_dividend_date = Column(String, nullable=True, index=True)
    record_date     = Column(String, nullable=True)
    pay_date        = Column(String, nullable=True)
    amount          = Column(Numeric(18, 4), nullable=True)
    currency        = Column(String(10), nullable=True)   # e.g. "NGN"
    frequency       = Column(String(20), nullable=True)   # e.g. "Annual", "Interim"

    stock = relationship("Stock", back_populates="dividends")


# ── Stock Executives ──────────────────────────────────────────────────────────


class StockExecutive(Base):
    """
    Key executives and management team for a company.
    """
    __tablename__ = "stock_executives"

    id       = Column(Integer, primary_key=True, index=True)
    stock_id = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    name     = Column(String(100), nullable=False)
    title    = Column(String(200), nullable=True)
    age      = Column(Integer, nullable=True)
    since    = Column(String(50), nullable=True)

    stock = relationship("Stock", back_populates="executives")


# ── Market Cap History ───────────────────────────────────────────────────────


class MarketCapHistory(Base):
    """
    Historical market capitalization for a stock.
    One row per (stock, date). Frequency indicates how the data was sourced.
    """
    __tablename__ = "market_cap_history"
    __table_args__ = (UniqueConstraint("stock_id", "date", name="uq_mcap_stock_date"),)

    id        = Column(Integer, primary_key=True, index=True)
    stock_id  = Column(Integer, ForeignKey("stocks.id"), nullable=False, index=True)
    date      = Column(String, nullable=False, index=True)
    market_cap = Column(Numeric(28, 2), nullable=True)
    frequency = Column(String(20), nullable=True)   # e.g. "daily", "annual"

    stock = relationship("Stock", back_populates="market_cap_history")
