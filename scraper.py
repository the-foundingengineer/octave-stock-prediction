"""
scraper.py  ─  Complete NGX scraper  (stockanalysis.com)
=========================================================

Covers EVERY page and sub-page for each Nigerian stock:

  PAGE                               WHAT IT YIELDS
  ─────────────────────────────────────────────────────────────────────────────
  /quote/ngx/{SYM}/                  -> overview KPIs, company snapshot
  /quote/ngx/{SYM}/history/          -> OHLCV daily price history
  /quote/ngx/{SYM}/dividend/         -> dividend stats + full payment history
  /quote/ngx/{SYM}/financials/                    ─┐
  /quote/ngx/{SYM}/financials/balance-sheet/       ├─ annual financials
  /quote/ngx/{SYM}/financials/cash-flow-statement/ ┘
  /quote/ngx/{SYM}/financials/?p=quarterly          ─┐
  /quote/ngx/{SYM}/financials/balance-sheet/?p=quarterly  ├─ quarterly
  /quote/ngx/{SYM}/financials/cash-flow-statement/?p=quarterly ┘
  /quote/ngx/{SYM}/financials/ratios/              -> ratio time-series
  /quote/ngx/{SYM}/statistics/                     -> deep valuation snapshot
  /quote/ngx/{SYM}/metrics/                        -> KPI time-series table
  /quote/ngx/{SYM}/market-cap/                     ─┐
  /quote/ngx/{SYM}/revenue/                         │
  /quote/ngx/{SYM}/net-income/                      │
  /quote/ngx/{SYM}/eps/                             ├─ individual metric history
  /quote/ngx/{SYM}/pe-ratio/                        │   -> metric_history table
  /quote/ngx/{SYM}/shares/                          │
  /quote/ngx/{SYM}/free-cash-flow/                 ─┘
  /quote/ngx/{SYM}/forecast/                        -> analyst consensus + estimates
  /quote/ngx/{SYM}/ratings/ (or /forecast/)         -> individual analyst ratings
  /quote/ngx/{SYM}/company/                         -> profile + executives
  /quote/ngx/{SYM}/employees/                       -> headcount history

Link discovery:
  The scraper first loads the overview page and walks every <a href> that
  lives under /quote/ngx/{SYMBOL}/. Any discovered path that is NOT in the
  explicit list above is also fetched and its table data stored in
  metric_history under the slug name. This means future pages added by
  stockanalysis.com are automatically captured.

Usage:
    python scraper.py                        # full run, all NGX stocks
    python scraper.py --symbol MTNN          # single stock
    python scraper.py --skip-history         # skip OHLCV (faster)
    python scraper.py --quarterly            # include quarterly financials
    python scraper.py --workers 3            # parallel workers (be careful)

Requirements:
    pip install requests beautifulsoup4 sqlalchemy lxml
"""

import argparse
import time
import traceback
from concurrent.futures import ThreadPoolExecutor, as_completed
# - Financials:  /quote/ngx/{SYM}/financials
# - Statistics:  /quote/ngx/{SYM}/statistics
# - Dividends:   /quote/ngx/{SYM}/dividend
# - Profile:     /quote/ngx/{SYM}/company
# - Market Cap:  /quote/ngx/{SYM}/market-cap
# - Metrics:     /quote/ngx/{SYM}/metrics (if exists)
# - Employees:   /quote/ngx/{SYM}/employees
from datetime import date, datetime
from typing import Optional, List, Dict
from urllib.parse import urljoin

import traceback
import requests
from bs4 import BeautifulSoup
from sqlalchemy import cast, Date

from app.database import engine, SessionLocal as Session



from app.models import (
    Base, Stock, DailyKline, Dividend,
    IncomeStatement, BalanceSheet, CashFlow, StockRatio,
    StockMetric, MetricHistory,
    AnalystRating, AnalystForecast,
    StockExecutive,
    MarketCapHistory,
    EmployeeHistory
)


# Config
# -----------------------------------------------------------------------------
BASE_URL      = "https://stockanalysis.com"

NGX_LIST_URL  = f"{BASE_URL}/list/nigerian-stock-exchange/"
REQUEST_DELAY = 1.5   # seconds between requests

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Metric-history pages: slug -> metric_name stored in metric_history table
METRIC_PAGES = {
    "market-cap":     "market_cap",
    "revenue":        "revenue",
    "net-income":     "net_income",
    "eps":            "eps",
    "pe-ratio":       "pe_ratio",
    "shares":         "shares_outstanding",
    "free-cash-flow": "free_cash_flow",
    "employees":      "employees",   # also parsed separately for employee_history
}

Base.metadata.create_all(engine)



# -----------------------------------------------------------------------------
# HTTP + parsing utilities
# -----------------------------------------------------------------------------

def fetch(url: str) -> Optional[BeautifulSoup]:
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code == 404:
            return None
        res.raise_for_status()
        return BeautifulSoup(res.text, "html.parser")
    except Exception as exc:
        print(f"    FETCH ERROR {url}: {exc}")
        return None


def parse_date(s) -> Optional[date]:
    if not s:
        return None
    s = str(s).strip()
    if s in ("", "n/a", "-", "—"):
        return None
    for fmt in ("%b %d, %Y", "%B %d, %Y", "%Y-%m-%d", "%m/%d/%Y", "%b %Y", "%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def parse_number(s) -> Optional[float]:
    if not s:
        return None
    s = str(s).strip().replace(",", "").replace("%", "").replace("$", "")
    if s in ("", "n/a", "-", "—", "N/A", "None"):
        return None
    mult = {"T": 1e12, "B": 1e9, "M": 1e6, "K": 1e3}
    if s and s[-1].upper() in mult:
        try:
            return float(s[:-1]) * mult[s[-1].upper()]
        except ValueError:
            pass
    try:
        return float(s)
    except ValueError:
        return None


def parse_amount_currency(s: str):
    if not s or s.strip() in ("", "n/a"):
        return None, None
    parts = s.strip().split()
    try:
        return float(parts[0].replace(",", "")), (parts[1] if len(parts) > 1 else "NGN")
    except (ValueError, IndexError):
        return parse_number(s), "NGN"


def extract_kv_pairs(soup: BeautifulSoup) -> dict[str, str]:
    """
    Generic extractor: walk every <tr> and every label/value <div> pair
    and return {label_text: value_text}.
    Handles both table-based and div-based layouts.
    """
    pairs = {}

    # Strategy A: <tr> with two <td>
    for row in soup.find_all("tr"):
        tds = row.find_all("td")
        if len(tds) >= 2:
            label = tds[0].get_text(strip=True)
            value = tds[1].get_text(strip=True)
            if label:
                pairs[label] = value

    # Strategy B: div containers where a text node matches a known label
    # (some pages use flex/grid divs rather than tables)
    for div in soup.find_all("div"):
        children = [c for c in div.children if hasattr(c, "get_text")]
        if len(children) == 2:
            label = children[0].get_text(strip=True)
            value = children[1].get_text(strip=True)
            if label and value and label not in pairs:
                pairs[label] = value

    return pairs


def parse_main_table(soup: BeautifulSoup) -> tuple[list[str], list[dict]]:
    """
    Parse the standard 'main table' used on financial / history pages.
    Returns (date_headers, rows) where each row is {label, date_header: value}.
    """
    table = soup.find("table", id="main-table") or soup.find("table")
    if not table:
        return [], []

    thead = table.find("thead")
    date_headers = []
    if thead:
        ths = thead.find_all("th")
        date_headers = [th.get_text(strip=True) for th in ths[1:]]  # skip label col

    rows = []
    tbody = table.find("tbody")
    for tr in (tbody or table).find_all("tr"):
        cells = tr.find_all("td")
        if not cells:
            continue
        row = {"label": cells[0].get_text(strip=True)}
        for i, td in enumerate(cells[1:]):
            if i < len(date_headers):
                row[date_headers[i]] = td.get_text(strip=True)
        rows.append(row)

    return date_headers, rows


# ─────────────────────────────────────────────────────────────────────────────
# Link discovery
# ─────────────────────────────────────────────────────────────────────────────
def discover_pages(symbol: str, soup: BeautifulSoup) -> dict[str, str]:
    """
    Walk every <a href> on the overview page that starts with the stock's
    base path. Returns {slug: absolute_url}.

    E.g. {"financials": "https://.../financials/",
          "market-cap": "https://.../market-cap/", ...}
    """
    prefix = f"/quote/ngx/{symbol.upper()}/"
    pages  = {}
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if href.startswith(prefix) and href != prefix:
            slug     = href[len(prefix):].strip("/")
            full_url = urljoin(BASE_URL, href)
            if slug and slug not in pages:
                pages[slug] = full_url

    # Always include the overview itself
    pages["overview"] = f"{BASE_URL}{prefix}"
    return pages


# ─────────────────────────────────────────────────────────────────────────────
# Stock list
# ─────────────────────────────────────────────────────────────────────────────
def fetch_stock_list() -> list[dict]:
    print("Fetching NGX stock list ...")
    soup = fetch(NGX_LIST_URL)
    if not soup:
        raise RuntimeError("Cannot reach stock list page.")

    table = soup.find("table")
    if not table:
        raise RuntimeError("No <table> on stock list page.")

    thead   = table.find("thead")
    headers = [th.get_text(strip=True).lower() for th in thead.find_all("th")] if thead else []
    col     = {h: i for i, h in enumerate(headers)}

    def cell(cells, key):
        idx = col.get(key)
        return cells[idx].get_text(strip=True) if idx is not None and idx < len(cells) else ""

    stocks = []
    for row in table.find("tbody").find_all("tr"):
        cells  = row.find_all("td")
        if not cells:
            continue
            
        sym_idx = col.get("symbol")
        if sym_idx is not None and sym_idx < len(cells):
            link   = cells[sym_idx].find("a")
            symbol = link.get_text(strip=True) if link else cells[sym_idx].get_text(strip=True)
        else:
            continue # should not happen
            
        name = cell(cells, "company name") or cell(cells, "name")
        
        stocks.append({
            "symbol":     symbol,
            "name":       name,
            "price":      parse_number(cell(cells, "stock price") or cell(cells, "price")),
            "market_cap": parse_number(cell(cells, "market cap") or cell(cells, "mkt cap")),
            "sector":     cell(cells, "sector") or cell(cells, "industry"),
        })

    print(f"  -> {len(stocks)} stocks found.")
    return stocks


# ─────────────────────────────────────────────────────────────────────────────
# Individual page scrapers
# ─────────────────────────────────────────────────────────────────────────────

# Label -> (model_field, parser)  used by overview and statistics
OVERVIEW_MAP = {
    "market cap":          ("market_cap",         parse_number),
    "enterprise value":    ("enterprise_value",   parse_number),
    "revenue (ttm)":       ("revenue_ttm",        parse_number),   # stored on stock temporarily
    "net income (ttm)":    ("net_income_ttm",     parse_number),
    "shares out":          ("shares_outstanding", parse_number),
    "eps (ttm)":           ("eps_ttm",            parse_number),
    "pe ratio":            ("pe_ratio",           parse_number),
    "forward pe":          ("forward_pe",         parse_number),
    "dividend":            ("annual_dividend",    lambda v: parse_number(v.split("(")[0].strip())),
    "ex-dividend date":    ("ex_div_date_str",    str),
    "beta":                ("beta",               parse_number),
    "52-week high":        ("week_52_high",       parse_number),
    "52-week low":         ("week_52_low",        parse_number),
    "price target":        ("price_target",       parse_number),
    "analyst consensus":   ("analyst_consensus",  str),
    "analysts":            ("analyst_consensus",  str),
    "volume":              ("_skip",              None),
    "employees":           ("employees",          lambda v: int(parse_number(v) or 0)),
    "ipo date":            ("ipo_date_str",       str),
    "stock exchange":      ("stock_exchange",     str),
}

STAT_MAP = {
    # Valuation
    "market cap":               ("market_cap",          parse_number),
    "enterprise value":         ("enterprise_value",    parse_number),
    "p/e ratio":                ("pe_ratio",            parse_number),
    "forward p/e":              ("forward_pe",          parse_number),
    "p/s ratio":                ("ps_ratio",            parse_number),
    "p/b ratio":                ("pb_ratio",            parse_number),
    "p/fcf ratio":              ("pc_ratio",            parse_number),
    "peg ratio":                ("peg_ratio",           parse_number),
    "ev/ebitda":                ("ev_ebitda",           parse_number),
    "ev/sales":                 ("ev_sales",            parse_number),
    "ev/fcf":                   ("ev_fcf",              parse_number),
    "ev/ebit":                  ("ev_ebit",             parse_number),
    # Share stats
    "shares outstanding":       ("shares_outstanding",  parse_number),
    "float":                    ("float_shares",        parse_number),
    "shares short":             ("shares_short",        parse_number),
    "short ratio":              ("short_ratio",         parse_number),
    "insider ownership":        ("insider_ownership",   parse_number),
    "institutional ownership":  ("institutional_ownership", parse_number),
    # Profitability
    "return on equity":         ("roe",                 parse_number),
    "return on assets":         ("roa",                 parse_number),
    "return on capital":        ("roic",                parse_number),
    "return on invested":       ("roic",                parse_number),
    "return on employed":       ("roce",                parse_number),
    "gross margin":             ("gross_margin",        parse_number),
    "operating margin":         ("operating_margin",    parse_number),
    "profit margin":            ("profit_margin",       parse_number),
    "fcf margin":               ("fcf_margin",          parse_number),
    "ebitda margin":            ("ebitda_margin",       parse_number),
    "asset turnover":           ("asset_turnover",      parse_number),
    "inventory turnover":       ("inventory_turnover",  parse_number),
    # Liquidity & Debt
    "current ratio":            ("current_ratio",       parse_number),
    "quick ratio":              ("quick_ratio",         parse_number),
    "debt / equity":            ("debt_equity",         parse_number),
    "debt / ebitda":            ("debt_ebitda",         parse_number),
    "debt / fcf":               ("debt_fcf",            parse_number),
    "net debt":                 ("net_debt",            parse_number),
    "interest coverage":        ("interest_coverage",   parse_number),
    # Dividends
    "dividend yield":           ("dividend_yield",      parse_number),
    "5-year avg yield":         ("dividend_yield_5yr",  parse_number),
    "payout ratio":             ("payout_ratio",        parse_number),
    "3-year div growth":        ("dividend_growth_3yr", parse_number),
    "5-year div growth":        ("dividend_growth_5yr", parse_number),
    "years of growth":          ("years_of_growth",     lambda v: int(parse_number(v) or 0)),
    # Growth
    "revenue growth (1y)":      ("revenue_growth_1yr",  parse_number),
    "revenue growth (3y)":      ("revenue_growth_3yr",  parse_number),
    "revenue growth (5y)":      ("revenue_growth_5yr",  parse_number),
    "eps growth (1y)":          ("eps_growth_1yr",      parse_number),
    "eps growth (3y)":          ("eps_growth_3yr",      parse_number),
    "eps growth (5y)":          ("eps_growth_5yr",      parse_number),
    "fcf growth (1y)":          ("fcf_growth_1yr",      parse_number),
    # Health
    "altman z-score":           ("altman_z_score",      parse_number),
    "piotroski f-score":        ("piotroski_f_score",   lambda v: int(parse_number(v) or 0)),
    # Performance
    "52-week high":             ("week_52_high",        parse_number),
    "52-week low":              ("week_52_low",         parse_number),
    "52-week change":           ("week_52_change",      parse_number),
    "beta":                     ("beta",                parse_number),
    "average volume":           ("average_volume",      parse_number),
    "average volume (10d)":     ("average_volume_10d",  parse_number),
}

# Financial statement row label -> model field
FINANCIAL_MAP = {
    # Income
    "revenue":                      "revenue",
    "revenue growth":               "revenue_growth_yoy",
    "cost of revenue":              "cost_of_revenue",
    "gross profit":                 "gross_profit",
    "gross margin":                 "gross_margin",
    "sg&a expenses":                "sga_expenses",
    "operating income":             "operating_income",
    "operating margin":             "operating_margin",
    "interest expense":             "interest_expense",
    "pretax income":                "pretax_income",
    "income tax":                   "income_tax",
    "net income":                   "net_income",
    "net income growth":            "net_income_growth_yoy",
    "ebitda":                       "ebitda",
    "ebitda margin":                "ebitda_margin",
    "eps (basic)":                  "eps_basic",
    "eps (diluted)":                "eps_diluted",
    "eps growth":                   "eps_growth_yoy",
    "shares (basic)":               "shares_basic",
    "shares (diluted)":             "shares_diluted",

    # Balance
    "cash & equivalents":           "cash_equivalents",
    "short-term investments":       "short_term_investments",
    "receivables":                  "accounts_receivable",
    "inventory":                    "inventory",
    "total current assets":         "total_current_assets",
    "property, plant":              "ppe",
    "long-term investments":        "long_term_investments",
    "goodwill":                     "goodwill",
    "total assets":                 "total_assets",
    "accounts payable":             "accounts_payable",
    "short-term debt":              "short_term_debt",
    "total current liabilities":    "total_current_liabilities",
    "long-term debt":               "long_term_debt",
    "total liabilities":            "total_liabilities",
    "common stock":                 "common_stock",
    "retained earnings":            "retained_earnings",
    "total equity":                 "shareholders_equity",
    "total debt":                   "total_debt",
    "book value / share":           "book_value_per_share",

    # Cash Flow
    "depreciation":                 "depreciation_amortization",
    "stock-based compensation":     "stock_based_compensation",
    "change in working capital":    "change_in_working_capital",
    "other operating activities":   "other_operating_activities",
    "operating cash flow":          "operating_cash_flow",
    "capital expenditures":         "capex",
    "acquisitions":                 "acquisitions",
    "purchases of investments":     "purchases_investments",
    "sales of investments":         "sales_investments",
    "other investing activities":   "other_investing_activities",
    "investing cash flow":          "investing_cash_flow",
    "dividends paid":               "dividends_paid",
    "share issuance":               "share_issuance",
    "debt issuance":                "debt_issuance",
    "debt repayment":               "debt_repayment",
    "other financing activities":   "other_financing_activities",
    "financing cash flow":          "financing_cash_flow",
    "net change in cash":           "net_change_in_cash",
    "free cash flow":               "free_cash_flow",
    "fcf per share":                "fcf_per_share",
}

RATIO_MAP = {
    "p/e ratio":         "pe_ratio",
    "p/s ratio":         "ps_ratio",
    "p/b ratio":         "pb_ratio",
    "p/fcf":             "p_fcf_ratio",
    "peg ratio":         "peg_ratio",
    "ev/ebitda":         "ev_ebitda",
    "ev/sales":          "ev_sales",
    "ev/fcf":            "ev_fcf",
    "ev/ebit":           "ev_ebit",
    "market cap":        "market_cap",
    "enterprise value":  "enterprise_value",
    "gross margin":      "gross_margin",
    "operating margin":  "operating_margin",
    "profit margin":     "profit_margin",
    "fcf margin":        "fcf_margin",
    "ebitda margin":     "ebitda_margin",
    "return on equity":  "roe",
    "return on assets":  "roa",
    "return on capital": "roic",
    "asset turnover":    "asset_turnover",
    "inventory turnover":"inventory_turnover",
    "current ratio":     "current_ratio",
    "quick ratio":       "quick_ratio",
    "debt / equity":     "debt_equity",
    "debt / ebitda":     "debt_ebitda",
    "debt / fcf":        "debt_fcf",
    "interest coverage": "interest_coverage",
    "eps (basic)":       "eps_basic",
    "eps (diluted)":     "eps_diluted",
    "book value":        "book_value_per_share",
    "fcf per share":     "fcf_per_share",

    "dividend per share":"dividend_per_share",
    "revenue per share": "revenue_per_share",
}

METRIC_TABLE_MAP = {
    "stock price":       "stock_price",
    "market cap":        "market_cap",
    "revenue":           "revenue",
    "gross profit":      "gross_profit",
    "operating income":  "operating_income",
    "net income":        "net_income",
    "ebitda":            "ebitda",
    "eps (basic)":       "eps_basic",
    "eps (diluted)":     "eps_diluted",
    "free cash flow":    "free_cash_flow",
    "operating cash flow":"operating_cf",
    "total assets":      "total_assets",
    "total debt":        "total_debt",
    "total equity":      "total_equity",
    "shares (basic)":    "shares_basic",
    "shares (diluted)":  "shares_diluted",
    "dividend per share":"dividend_per_share",
    "pe ratio":          "pe_ratio",
    "pb ratio":          "pb_ratio",
    "ps ratio":          "ps_ratio",
    "ev/ebitda":         "ev_ebitda",
    "return on equity":  "roe",
    "return on assets":  "roa",
    "gross margin":      "gross_margin",
    "operating margin":  "operating_margin",
    "profit margin":     "profit_margin",
}


def _match_label(label: str, mapping: dict) -> Optional[str]:
    """Case-insensitive partial match against a label->field mapping."""
    low = label.lower()
    for key, field in mapping.items():
        if key in low:
            return field
    return None


# ── Overview ──────────────────────────────────────────────────────────────────
def scrape_overview(soup: BeautifulSoup) -> dict:
    kv   = extract_kv_pairs(soup)
    data = {}
    for raw_label, raw_value in kv.items():
        low = raw_label.lower()
        for map_label, (field, fn) in OVERVIEW_MAP.items():
            if map_label in low and field != "_skip":
                try:
                    data[field] = fn(raw_value)
                except Exception:
                    pass
                break

    # Also grab company info block (sector, industry, IPO date, exchange)
    for node in soup.find_all(string=True):
        txt = node.strip()
        parent_text = node.parent.get_text(strip=True) if node.parent else ""
        if "Sector" in txt:
            a = node.parent.find("a") if node.parent else None
            if a:
                data["sector"] = a.get_text(strip=True)
        if "Industry" in txt:
            a = node.parent.find("a") if node.parent else None
            if a:
                data["industry"] = a.get_text(strip=True)

    return data


# ── Price history ─────────────────────────────────────────────────────────────
def scrape_history(url: str) -> list[dict]:
    soup = fetch(url)
    if not soup:
        return []
    _, rows = parse_main_table(soup)
    result  = []
    for row in rows:
        dt = parse_date(row.get("label") or row.get("Date"))
        if not dt:
            # Try first value column as date
            vals = [v for k, v in row.items() if k != "label"]
            dt   = parse_date(vals[0]) if vals else None
        if not dt:
            continue
        cols = [v for k, v in row.items() if k != "label"]
        result.append({
            "date":       dt,
            "open":       parse_number(cols[0]) if len(cols) > 0 else None,
            "high":       parse_number(cols[1]) if len(cols) > 1 else None,
            "low":        parse_number(cols[2]) if len(cols) > 2 else None,
            "close":      parse_number(cols[3]) if len(cols) > 3 else None,
            "volume":     parse_number(cols[4]) if len(cols) > 4 else None,
            "change_pct": parse_number(cols[5]) if len(cols) > 5 else None,
        })
    return result


# ── Dividends ─────────────────────────────────────────────────────────────────
def scrape_dividends(url: str) -> dict:
    soup = fetch(url)
    if not soup:
        return {"stats": {}, "history": []}

    # Stats section (label/value pairs above the table)
    kv    = extract_kv_pairs(soup)
    stats = {}
    stat_fields = {
        "dividend yield":   ("dividend_yield",   parse_number),
        "annual dividend":  ("annual_dividend",  lambda v: parse_number(v.split()[0])),
        "ex-dividend date": ("ex_dividend_date", str),
        "payout frequency": ("payout_frequency", str),
        "payout ratio":     ("payout_ratio",     parse_number),
        "dividend growth":  ("dividend_growth",  parse_number),
    }
    for raw_label, raw_value in kv.items():
        low = raw_label.lower()
        for key, (field, fn) in stat_fields.items():
            if key in low:
                try:
                    stats[field] = fn(raw_value)
                except Exception:
                    pass
                break

    # History table
    _, rows = parse_main_table(soup)
    history = []
    for row in rows:
        cols = list(row.values())  # label col + date cols
        # table columns: Ex-Date | Amount | Record Date | Pay Date
        ex_date = parse_date(cols[0]) if cols else None
        if not ex_date:
            continue
        amount, currency = parse_amount_currency(cols[1]) if len(cols) > 1 else (None, "NGN")
        history.append({
            "ex_dividend_date": ex_date,
            "amount":           amount,
            "currency":         currency,
            "record_date":      parse_date(cols[2]) if len(cols) > 2 else None,
            "pay_date":         parse_date(cols[3]) if len(cols) > 3 else None,
        })

    return {"stats": stats, "history": history}


# ── Financials (income / balance / cash flow) ─────────────────────────────────
def scrape_financial_page(url: str, period_type: str) -> dict[str, dict]:
    """Returns {period_end_str: {field: value}}"""
    soup = fetch(url)
    if not soup:
        return {}
    date_headers, rows = parse_main_table(soup)
    periods = {}
    for dh in date_headers:
        dt  = parse_date(dh)
        key = str(dt) if dt else dh
        if key not in periods:
            periods[key] = {"period_end": dt, "period_type": period_type}
    for row in rows:
        label = row.get("label", "").lower()
        field = None
        for map_label, map_field in FINANCIAL_MAP.items():
            if map_label in label:
                field = map_field
                break
        if not field:
            continue
        for dh in date_headers:
            dt  = parse_date(dh)
            key = str(dt) if dt else dh
            if key in periods and dh in row:
                val = parse_number(row[dh])
                if val is not None:
                    periods[key][field] = val
    return periods


def scrape_all_financials(base_url: str, include_quarterly: bool) -> list[dict]:
    """Merge income + balance + cash flow for annual (+ optional quarterly)."""
    all_periods: dict[str, dict] = {}
    slugs = [
        ("financials/", "annual"),
        ("financials/balance-sheet/", "annual"),
        ("financials/cash-flow-statement/", "annual"),
    ]
    if include_quarterly:
        slugs += [
            ("financials/?p=quarterly", "quarterly"),
            ("financials/balance-sheet/?p=quarterly", "quarterly"),
            ("financials/cash-flow-statement/?p=quarterly", "quarterly"),
        ]
    for slug, ptype in slugs:
        page_periods = scrape_financial_page(f"{base_url}{slug}", ptype)
        for k, v in page_periods.items():
            if k not in all_periods:
                all_periods[k] = v
            else:
                all_periods[k].update({fk: fv for fk, fv in v.items() if fv is not None})
        time.sleep(REQUEST_DELAY)
    return list(all_periods.values())


# ── Financial Ratios ──────────────────────────────────────────────────────────
def scrape_ratios(url: str) -> list[dict]:
    soup = fetch(url)
    if not soup:
        return []
    date_headers, rows = parse_main_table(soup)
    periods = {dh: {"period_end": parse_date(dh), "period_type": "annual"} for dh in date_headers}
    for row in rows:
        label = row.get("label", "").lower()
        field = _match_label(label, RATIO_MAP)
        if not field:
            continue
        for dh in date_headers:
            if dh in row and dh in periods:
                val = parse_number(row[dh])
                if val is not None:
                    periods[dh][field] = val
    return list(periods.values())


# ── Statistics ────────────────────────────────────────────────────────────────
def scrape_statistics(url: str) -> dict:
    soup = fetch(url)
    if not soup:
        return {}
    kv   = extract_kv_pairs(soup)
    data = {"as_of": date.today()}
    for raw_label, raw_value in kv.items():
        low = raw_label.lower()
        for map_label, (field, fn) in STAT_MAP.items():
            if map_label in low:
                try:
                    data[field] = fn(raw_value)
                except Exception:
                    pass
                break
    return data


# ── Metrics page (/metrics/) ──────────────────────────────────────────────────
def scrape_metrics(url: str) -> list[dict]:
    soup = fetch(url)
    if not soup:
        return []
    date_headers, rows = parse_main_table(soup)
    periods = {dh: {"period_end": parse_date(dh)} for dh in date_headers}
    for row in rows:
        label = row.get("label", "").lower()
        field = _match_label(label, METRIC_TABLE_MAP)
        if not field:
            continue
        for dh in date_headers:
            if dh in row and dh in periods:
                val = parse_number(row[dh])
                if val is not None:
                    periods[dh][field] = val
    return list(periods.values())


# ── Individual metric history pages (/market-cap/, /revenue/, ...) ─────────────
def scrape_metric_history(url: str, metric_name: str) -> list[dict]:
    """
    Each dedicated metric page has one table: Year | Value | (Change %).
    Returns [{period_end, metric_name, value, change_pct}].
    """
    soup = fetch(url)
    if not soup:
        return []
    headers, rows = parse_main_table(soup)
    result  = []
    for row in rows:
        label = row.get("label")
        dt = parse_date(label)
        if not dt:
            continue
        
        # Determine which header is the 'value' and which is 'change'
        # Headers like 'Revenue', 'Market Cap', 'EPS' are values
        # Headers ending in '%' or containing 'Change' or 'Growth' are changes
        val = None
        change = None
        
        for h in headers:
            if h not in row: continue
            h_low = h.lower()
            if "%" in h_low or "change" in h_low or "growth" in h_low:
                change = parse_number(row[h])
            else:
                # Assume first non-change header is the value
                if val is None:
                    val = parse_number(row[h])
        
        result.append({
            "period_end":  dt,
            "metric_name": metric_name,
            "value":       val,
            "change_pct":  change,
        })
    return result



# ── Forecast / Analyst ratings ────────────────────────────────────────────────
def scrape_forecast(url: str) -> tuple[dict, list[dict]]:
    """Returns (consensus_dict, [individual_rating_dicts])."""
    soup = fetch(url)
    if not soup:
        return {}, []

    # Consensus summary (KV pairs)
    kv       = extract_kv_pairs(soup)
    forecast = {"as_of": date.today()}
    forecast_fields = {
        "consensus":           ("consensus",        str),
        "analyst":             ("num_analysts",     lambda v: int(parse_number(v) or 0)),
        "price target":        ("price_target_avg", parse_number),
        "high":                ("price_target_high",parse_number),
        "low":                 ("price_target_low", parse_number),
        "upside":              ("upside_pct",       parse_number),
        "eps estimate":        ("eps_estimate_cur_yr", parse_number),
        "revenue estimate":    ("rev_estimate_cur_yr", parse_number),
    }
    for raw_label, raw_value in kv.items():
        low = raw_label.lower()
        for key, (field, fn) in forecast_fields.items():
            if key in low:
                try:
                    forecast[field] = fn(raw_value)
                except Exception:
                    pass
                break

    # Individual analyst ratings table
    ratings = []
    _, rows  = parse_main_table(soup)
    for row in rows:
        vals = list(row.values())
        if len(vals) < 3:
            continue
        ratings.append({
            "analyst_firm": vals[0] if len(vals) > 0 else None,
            "rating_date":  parse_date(vals[1]) if len(vals) > 1 else None,
            "rating":       vals[2] if len(vals) > 2 else None,
            "price_target": parse_number(vals[3]) if len(vals) > 3 else None,
            "action":       vals[4] if len(vals) > 4 else None,
        })

    return forecast, ratings


# ── Company profile + executives ──────────────────────────────────────────────
def scrape_company(url: str) -> dict:
    soup = fetch(url)
    if not soup:
        return {}

    # Description: longest <p> with enough text
    description = None
    for p in sorted(soup.find_all("p"), key=lambda x: len(x.get_text()), reverse=True):
        if len(p.get_text()) > 100:
            description = p.get_text(strip=True)
            break

    # KV profile info
    profile   = {}
    kv        = extract_kv_pairs(soup)
    prof_keys = {
        "headquartered": "headquarters",
        "headquarters":  "headquarters",
        "founded":       "founded",
        "employees":     "employees",
        "website":       "website",
        "industry":      "industry",
        "sector":        "sector",
        "ipo":           "ipo_date_str",
        "exchange":      "stock_exchange",
        "isin":          "isin",
    }
    for raw_label, raw_value in kv.items():
        low = raw_label.lower()
        for key, field in prof_keys.items():
            if key in low:
                if field == "employees":
                    try:
                        profile[field] = int(parse_number(raw_value) or 0)
                    except Exception:
                        pass
                else:
                    profile[field] = raw_value
                break

    # Executives table — usually has Name, Title, Age, Since columns
    executives = []
    for table in soup.find_all("table"):
        tbody = table.find("tbody")
        if not tbody:
            continue
        for tr in tbody.find_all("tr"):
            cols = [td.get_text(strip=True) for td in tr.find_all("td")]
            if len(cols) >= 2 and cols[0]:
                executives.append({
                    "name":  cols[0],
                    "title": cols[1] if len(cols) > 1 else None,
                    "age":   int(cols[2]) if len(cols) > 2 and cols[2].isdigit() else None,
                    "since": cols[3] if len(cols) > 3 else None,
                })
        if executives:
            break

    return {"description": description, **profile, "executives": executives}


# ── Employee history (/employees/) ────────────────────────────────────────────
def scrape_employees(url: str) -> list[dict]:
    rows = scrape_metric_history(url, "employees")
    return [
        {"period_end": r["period_end"],
         "employees":  int(r["value"]) if r["value"] else None,
         "change_pct": r["change_pct"]}
        for r in rows if r["period_end"]
    ]


# ─────────────────────────────────────────────────────────────────────────────
# DB upsert helpers
# ─────────────────────────────────────────────────────────────────────────────
def upsert_stock(db, symbol: str, name: str, extra: dict = None) -> Stock:
    stock = db.query(Stock).filter_by(symbol=symbol).first()
    if not stock:
        stock = Stock(symbol=symbol, name=name)
        db.add(stock)
    else:
        stock.name = name
    if extra:
        for k, v in extra.items():
            if v is not None and hasattr(stock, k):
                setattr(stock, k, v)
    stock.last_updated = date.today()
    return stock


def save_prices(db, stock_id, rows):
    # DailyKline stores date as String (YYYY-MM-DD usually)
    # Scraper rows["date"] is python date object.
    # We need to convert.
    
    # Check existing dates
    existing = {r[0] for r in db.query(DailyKline.date).filter_by(stock_id=stock_id).all()}
    
    new_objs = []
    for r in rows:
        d_str = r["date"].isoformat()
        if d_str not in existing:
            new_objs.append({
                "stock_id": stock_id,
                "date": d_str,
                "open": r.get("open"),
                "high": r.get("high"),
                "low": r.get("low"),
                "close": r.get("close"),
                "volume": int(r.get("volume") or 0),
                # DailyKline has 'turnover', 'ma_50d' etc. Scraper doesn't provide them here.
            })
            existing.add(d_str)

    if new_objs:
        db.bulk_insert_mappings(DailyKline, new_objs)
    print(f"    prices: +{len(new_objs)} rows")


def save_dividends(db, stock_id, history, frequency=None):
    for item in history:
        ex_dt = item["ex_dividend_date"]
        ex_str = ex_dt.isoformat() if hasattr(ex_dt, "isoformat") else str(ex_dt)
        
        div = db.query(Dividend).filter_by(
            stock_id=stock_id, ex_dividend_date=ex_str
        ).first()
        if not div:
            div = Dividend(stock_id=stock_id, ex_dividend_date=ex_str)
            db.add(div)
        
        div.amount = item["amount"]
        div.currency = item.get("currency", "NGN")
        div.frequency = frequency
        
        rd = item.get("record_date")
        if rd:
            div.record_date = rd.isoformat() if hasattr(rd, "isoformat") else str(rd)
        pd = item.get("pay_date")
        if pd:
            div.pay_date = pd.isoformat() if hasattr(pd, "isoformat") else str(pd)

        div.frequency = frequency


def save_financials(db, stock_id, periods):
    # periods is a list of dicts. Each dict has "period_end", "period_type", and data fields.
    # We will try to upsert into IncomeStatement, BalanceSheet, and CashFlow
    # based on which fields are present in the dict and the model.
    
    models = [IncomeStatement, BalanceSheet, CashFlow]
    
    for p in periods:
        pe   = p.get("period_end")
        pt   = p.get("period_type", "annual")
        if not pe:
            continue
            
        # For each model, check if we need to create/update a record
        # (excluding metadata fields)
        
        # Upsert
        # Note: app/models.py uses 'period_ending' for date, scraper uses 'period_end'
        # We must map period_end -> period_ending
        
        for ModelClass in models:
            model_cols = {c.name for c in ModelClass.__table__.columns}
            relevant_data = {k: v for k, v in p.items() 
                             if k in model_cols and k not in ("period_end", "period_type", "stock_id", "id")}
            
            if not relevant_data:
                continue
            
            row = db.query(ModelClass).filter_by(
                stock_id=stock_id, period_ending=pe, period_type=pt
            ).first()
            
            if not row:
                row = ModelClass(stock_id=stock_id, period_ending=pe, period_type=pt)
                db.add(row)
                
            for k, v in relevant_data.items():
                setattr(row, k, v)


def save_financial_ratios(db, stock_id, periods):
    # Maps to StockRatio
    for p in periods:
        pe = p.get("period_end")
        pt = p.get("period_type", "annual")
        if not pe:
            continue
            
        rat = db.query(StockRatio).filter(
            StockRatio.stock_id == stock_id,
            cast(StockRatio.period_ending, Date) == pe,
            StockRatio.period_type == pt
        ).first()
        if not rat:
            rat = StockRatio(stock_id=stock_id, period_ending=pe, period_type=pt)
            db.add(rat)
            
        for k, v in p.items():
            if k not in ("period_end", "period_type") and v is not None and hasattr(rat, k):
                setattr(rat, k, v)


def save_statistics(db, stock_id, data):
    if not data:
        return
    # Map statistics to StockRatio with period_type="current"
    # and period_end = today (or data['as_of'])
    
    as_of = data.get("as_of", date.today())
    
    # We might want to overwrite the "current" ratio for this stock
    rat = db.query(StockRatio).filter(
        StockRatio.stock_id == stock_id,
        StockRatio.period_type == "current"
    ).first()
    
    if not rat:
        rat = StockRatio(stock_id=stock_id, period_type="current", period_ending=as_of)
        db.add(rat)
    else:
        rat.period_ending = as_of
        
    for k, v in data.items():
        if v is not None and hasattr(rat, k):
            setattr(rat, k, v)


def save_metrics(db, stock_id, periods):
    for p in periods:
        pe = p.get("period_end")
        if not pe:
            continue
        met = db.query(StockMetric).filter_by(stock_id=stock_id, period_end=pe).first()
        if not met:
            met = StockMetric(stock_id=stock_id, period_end=pe)
            db.add(met)
        for k, v in p.items():
            if k != "period_end" and v is not None and hasattr(met, k):
                setattr(met, k, v)


def save_metric_history(db, stock_id, rows):
    for r in rows:
        pe = r.get("period_end")
        mn = r.get("metric_name")
        if not pe or not mn:
            continue
            
        # 1. MarketCapHistory (date is String)
        if mn == "market_cap":
            d_str = pe.isoformat() if hasattr(pe, "isoformat") else str(pe)
            mcap = db.query(MarketCapHistory).filter_by(stock_id=stock_id, date=d_str).first()
            if not mcap:
                mcap = MarketCapHistory(stock_id=stock_id, date=d_str, frequency="history")
                db.add(mcap)
            mcap.market_cap = r.get("value")

        # 2. MetricHistory (period_end is Date)
        # Ensure pe is a date object for MetricHistory
        if isinstance(pe, str):
            pe = parse_date(pe)
        if not pe:
            continue
            
        mh = db.query(MetricHistory).filter(
            MetricHistory.stock_id == stock_id,
            MetricHistory.metric_name == mn,
            cast(MetricHistory.period_end, Date) == pe
        ).first()



        if not mh:
            mh = MetricHistory(stock_id=stock_id, metric_name=mn, period_end=pe)
            db.add(mh)
        mh.value      = r.get("value")
        mh.change_pct = r.get("change_pct")



def save_forecast(db, stock_id, forecast, ratings):
    # Consensus
    fc = db.query(AnalystForecast).filter_by(stock_id=stock_id).first()
    if not fc:
        fc = AnalystForecast(stock_id=stock_id)
        db.add(fc)
    for k, v in forecast.items():
        if v is not None and hasattr(fc, k):
            setattr(fc, k, v)

    # Individual ratings (upsert by firm + date)
    for r in ratings:
        firm = r.get("analyst_firm")
        rdt  = r.get("rating_date")
        rdt_str = rdt.isoformat() if hasattr(rdt, "isoformat") else str(rdt)
        if not firm:
            continue
        ar = db.query(AnalystRating).filter_by(
            stock_id=stock_id, analyst_firm=firm, rating_date=rdt_str
        ).first()
        if not ar:
            ar = AnalystRating(stock_id=stock_id, analyst_firm=firm, rating_date=rdt_str)
            db.add(ar)
        for k, v in r.items():
             if k != "rating_date" and v is not None and hasattr(ar, k):
                if hasattr(v, "isoformat"):
                    v = v.isoformat()
                setattr(ar, k, v)



def save_executives(db, stock_id, execs):
    db.query(StockExecutive).filter_by(stock_id=stock_id).delete()
    for e in execs:
        if e.get("name"):
            db.add(StockExecutive(stock_id=stock_id, **e))


def save_employee_history(db, stock_id, rows):
    for r in rows:
        pe = r.get("period_end")
        if not pe:
            continue
        # Ensure pe is a date object
        if isinstance(pe, str):
            pe = parse_date(pe)
        if not pe:
            continue
            
        eh = db.query(EmployeeHistory).filter_by(stock_id=stock_id, period_end=pe).first()
        if not eh:
            eh = EmployeeHistory(stock_id=stock_id, period_end=pe)
            db.add(eh)
        eh.employees  = r.get("employees")
        eh.change_pct = r.get("change_pct")



def synthesize_metrics(db, stock_id):
    """
    Populate StockMetric table from IncomeStatement, BalanceSheet, CashFlow, and StockRatio.
    This is useful when the /metrics/ page is missing or incomplete.
    """
    # 1. Identify all annual/FY period_end dates from IncomeStatement
    dates = db.query(IncomeStatement.period_ending).filter(
        IncomeStatement.stock_id == stock_id,
        IncomeStatement.period_type.in_(["annual", "FY"])
    ).all()

    dates = [d[0] for d in dates]
    
    print(f"    Synthesizing metrics for {len(dates)} periods...")
    
    for dt in dates:
        met = db.query(StockMetric).filter(
            StockMetric.stock_id == stock_id,
            cast(StockMetric.period_end, Date) == dt
        ).first()

        if not met:
            met = StockMetric(stock_id=stock_id, period_end=dt)
            db.add(met)
            
        # Fetch components
        inc = db.query(IncomeStatement).filter(
            IncomeStatement.stock_id == stock_id,
            IncomeStatement.period_ending == dt,
            IncomeStatement.period_type.in_(["annual", "FY"])
        ).first()
        bal = db.query(BalanceSheet).filter(
            BalanceSheet.stock_id == stock_id,
            BalanceSheet.period_ending == dt,
            BalanceSheet.period_type.in_(["annual", "FY"])
        ).first()
        cf  = db.query(CashFlow).filter(
            CashFlow.stock_id == stock_id,
            CashFlow.period_ending == dt,
            CashFlow.period_type.in_(["annual", "FY"])
        ).first()
        rat = db.query(StockRatio).filter(
            StockRatio.stock_id == stock_id,
            cast(StockRatio.period_ending, Date) == dt,
            StockRatio.period_type.in_(["annual", "FY"])
        ).first()


        
        # Populate fields
        if inc:
            met.revenue          = inc.revenue
            met.gross_profit     = inc.gross_profit
            met.operating_income = inc.operating_income
            met.net_income       = inc.net_income
            met.ebitda           = inc.ebitda
            met.eps_basic        = inc.eps_basic
            met.eps_diluted      = inc.eps_diluted
            met.gross_margin     = inc.gross_margin
            met.operating_margin = inc.operating_margin
            met.profit_margin    = inc.profit_margin
            met.dividend_per_share = inc.dividend_per_share

        if bal:
            met.total_assets = bal.total_assets
            met.total_debt   = bal.total_debt
            met.total_equity = bal.shareholders_equity
            met.shares_basic = bal.shares_outstanding # Approximation if not in IS

        
        if cf:
            met.free_cash_flow = cf.free_cash_flow
            met.operating_cf   = cf.operating_cash_flow
            
        if rat:
            met.pe_ratio    = rat.pe_ratio
            met.pb_ratio    = rat.pb_ratio
            met.ps_ratio    = rat.ps_ratio
            met.ev_ebitda   = rat.ev_ebitda
            met.roe         = rat.roe
            met.roa         = rat.roa
            met.market_cap  = rat.market_cap
        
        # Attempt to get stock price from DailyKline if missing
        if not met.stock_price:
            # Try exact date
            dk = db.query(DailyKline).filter_by(stock_id=stock_id, date=dt.isoformat()).first()
            if dk:
                met.stock_price = dk.close



# -----------------------------------------------------------------------------
# Main per-stock orchestrator
# -----------------------------------------------------------------------------

def scrape_one(symbol: str, name: str = "",
               skip_history: bool = False,
               include_quarterly: bool = False):

    print(f"\n=== {symbol} ===")

    db = Session()
    try:
        symbol   = symbol.upper()
        base_url = f"{BASE_URL}/quote/ngx/{symbol}/"

        # -- 1. Overview page (fetch once, use for KPIs + link discovery) ------

        overview_soup = fetch(base_url)
        if not overview_soup:
            print(f"    Skipping {symbol} — overview page not reachable.")
            return

        all_pages = discover_pages(symbol, overview_soup)
        print(f"    Discovered {len(all_pages)} sub-pages: {list(all_pages.keys())}")

        overview_data = scrape_overview(overview_soup)
        stock = upsert_stock(db, symbol, name or symbol, overview_data)
        db.flush()
        time.sleep(REQUEST_DELAY)

        # ── 2. Price history ────────────────────────────────────────────────
        if not skip_history and "history" in all_pages:
            prices = scrape_history(all_pages["history"])
            save_prices(db, stock.id, prices)
            time.sleep(REQUEST_DELAY)

        # ── 3. Dividends ────────────────────────────────────────────────────
        if "dividend" in all_pages:
            div_data = scrape_dividends(all_pages["dividend"])
            stats    = div_data["stats"]
            save_dividends(db, stock.id, div_data["history"],
                           frequency=stats.get("payout_frequency"))
            # propagate snapshot KPIs
            for f in ("dividend_yield", "annual_dividend", "payout_ratio"):
                if stats.get(f) is not None:
                    setattr(stock, f, stats[f])
            print(f"    dividends: {len(div_data['history'])} rows")
            time.sleep(REQUEST_DELAY)

        # ── 4. Financials (income + balance + cash flow, annual + quarterly) ─
        fin_periods = scrape_all_financials(base_url, include_quarterly)
        save_financials(db, stock.id, fin_periods)
        print(f"    financials: {len(fin_periods)} periods")

        # ── 5. Financial ratios ─────────────────────────────────────────────
        ratios_url = all_pages.get("financials/ratios") or f"{base_url}financials/ratios/"
        ratio_periods = scrape_ratios(ratios_url)
        save_financial_ratios(db, stock.id, ratio_periods)
        print(f"    ratios: {len(ratio_periods)} periods")
        time.sleep(REQUEST_DELAY)

        # ── 6. Statistics ───────────────────────────────────────────────────
        if "statistics" in all_pages:
            stat_data = scrape_statistics(all_pages["statistics"])
            save_statistics(db, stock.id, stat_data)
            print(f"    statistics: {len(stat_data)} fields")
            time.sleep(REQUEST_DELAY)

        # ── 7. Metrics time-series (/metrics/) ──────────────────────────────
        if "metrics" in all_pages:
            metric_periods = scrape_metrics(all_pages["metrics"])
            save_metrics(db, stock.id, metric_periods)
            print(f"    metrics: {len(metric_periods)} periods")
            time.sleep(REQUEST_DELAY)
        
        # Synthesize metrics from other tables (populate StockMetric)
        # relevant for Nigerian stocks where /metrics/ might be missing
        synthesize_metrics(db, stock.id)


        # ── 8. Individual metric history pages ──────────────────────────────
        for slug, metric_name in METRIC_PAGES.items():
            page_url = all_pages.get(slug) or f"{base_url}{slug}/"
            mh_rows  = scrape_metric_history(page_url, metric_name)
            save_metric_history(db, stock.id, mh_rows)
            print(f"    {slug}: {len(mh_rows)} history rows")
            time.sleep(REQUEST_DELAY)

        # ── 9. Employee history (separate table) ─────────────────────────────
        if "employees" in all_pages:
            emp_rows = scrape_employees(all_pages["employees"])
            save_employee_history(db, stock.id, emp_rows)
            if emp_rows:
                stock.employees = emp_rows[-1].get("employees")
            time.sleep(REQUEST_DELAY)

        # ── 10. Analyst forecast + ratings ───────────────────────────────────
        forecast_url = all_pages.get("forecast") or f"{base_url}forecast/"
        fc, ratings  = scrape_forecast(forecast_url)
        save_forecast(db, stock.id, fc, ratings)
        print(f"    forecast: consensus={fc.get('consensus')}, "
              f"{len(ratings)} analyst ratings")
        time.sleep(REQUEST_DELAY)

        # ── 11. Company profile + executives ─────────────────────────────────
        if "company" in all_pages:
            company = scrape_company(all_pages["company"])
            for f in ("description", "headquarters", "founded", "employees",
                      "website", "sector", "industry", "isin", "stock_exchange"):
                val = company.get(f)
                if val is not None and hasattr(stock, f):
                    setattr(stock, f, val)
            if company.get("ipo_date_str"):
                stock.ipo_date = parse_date(company["ipo_date_str"])
            save_executives(db, stock.id, company.get("executives", []))
            print(f"    company: {len(company.get('executives', []))} executives")
            time.sleep(REQUEST_DELAY)

        # ── 12. Any extra pages discovered but not explicitly handled above ──
        known_slugs = {
            "overview", "history", "dividend", "financials", "statistics",
            "metrics", "forecast", "company", "employees", "ratings",
            "financials/ratios",
            *METRIC_PAGES.keys(),
        }
        for slug, page_url in all_pages.items():
            if slug in known_slugs:
                continue
            print(f"    [extra] {slug} — storing table data as metric_history")
            extra_rows = scrape_metric_history(page_url, slug)
            save_metric_history(db, stock.id, extra_rows)
            time.sleep(REQUEST_DELAY)

        db.commit()
        print(f"    [OK] {symbol} committed.")

    except Exception as exc:
        db.rollback()
        print(f"    X ERROR {symbol}: {exc}")
        traceback.print_exc()
    finally:

        db.close()


# -----------------------------------------------------------------------------
# Runner
# -----------------------------------------------------------------------------

def scrape_all(skip_history=False, include_quarterly=False, workers=1):
    stocks = fetch_stock_list()
    total  = len(stocks)

    if workers > 1:
        with ThreadPoolExecutor(max_workers=workers) as pool:
            futs = {
                pool.submit(scrape_one, s["symbol"], s["name"],
                            skip_history, include_quarterly): s["symbol"]
                for s in stocks
            }
            for i, fut in enumerate(as_completed(futs), 1):
                sym = futs[fut]
                try:
                    fut.result()
                except Exception as exc:
                    print(f"[{i}/{total}] {sym} FAILED: {exc}")
    else:
        for i, s in enumerate(stocks, 1):
            print(f"\n[{i}/{total}]", end="")
            scrape_one(s["symbol"], s["name"], skip_history, include_quarterly)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Complete NGX stock scraper")
    ap.add_argument("--symbol",        help="Scrape a single symbol only")
    ap.add_argument("--skip-history",  action="store_true")
    ap.add_argument("--quarterly",     action="store_true",
                    help="Also scrape quarterly financials")
    ap.add_argument("--workers",       type=int, default=1,
                    help="Parallel workers (default 1 — be polite)")
    args = ap.parse_args()

    if args.symbol:
        scrape_one(args.symbol.upper(),
                   skip_history=args.skip_history,
                   include_quarterly=args.quarterly)
    else:
        scrape_all(skip_history=args.skip_history,
                   include_quarterly=args.quarterly,
                   workers=args.workers)