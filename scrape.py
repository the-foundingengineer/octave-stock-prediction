import os
import re
import time
import json
from datetime import datetime, date, timezone
from decimal import Decimal
from typing import Optional

from dotenv import load_dotenv
from sqlalchemy.orm import Session
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

from app.database import engine
from app.models import Base, Stock, DailyKline, IncomeStatement, BalanceSheet, CashFlow, StockRatio
from stock_codes import STOCK_CODES

load_dotenv()


# ─── 1. SCRAPER (unchanged) ───────────────────────────────────────────────────

def scrape_stock_data(driver, wait, symbol):
    base_url = f"https://stockanalysis.com/quote/ngx/{symbol}/"
    print(f"🔍 Scraping {symbol}...")
    data = {}

    driver.get(base_url)
    time.sleep(5)
    overview_all, overview_selected, dividends_full = {}, {}, {}
    for row in driver.find_elements(By.TAG_NAME, "tr"):
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) == 2:
            k, v = cols[0].text.strip(), cols[1].text.strip()
            overview_all[k] = v
            if k in ["Market Cap", "Revenue (ttm)", "Dividend Yield"]:
                overview_selected[k] = v
    try:
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        headers = driver.find_elements(By.XPATH, "//h2[contains(text(),'Dividend')]")
        if headers:
            tbl = headers[0].find_element(By.XPATH, "following::table[1]")
            for row in tbl.find_elements(By.TAG_NAME, "tr"):
                cols = row.find_elements(By.TAG_NAME, "td")
                if len(cols) == 2:
                    dividends_full[cols[0].text.strip()] = cols[1].text.strip()
        else:
            dividends_full["info"] = "No Dividend Section Found"
    except Exception as e:
        dividends_full["error"] = str(e)
    data["overview_full"]     = overview_all
    data["overview_selected"] = overview_selected
    data["dividends_full"]    = dividends_full

    driver.get(base_url + "financials/")
    time.sleep(5)
    sections = {}
    for section in ["Income Statement", "Balance Sheet", "Cash Flow", "Ratios", "KPIs"]:
        try:
            wait.until(EC.element_to_be_clickable((By.LINK_TEXT, section))).click()
            time.sleep(3)
            sections[section] = driver.find_element(By.TAG_NAME, "table").text
        except:
            sections[section] = "Not Found"
    data["financials"] = {"sections": sections}

    driver.get(base_url + "statistics/")
    time.sleep(5)
    stats_all, stats_selected = {}, {}
    for row in driver.find_elements(By.TAG_NAME, "tr"):
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) == 2:
            k, v = cols[0].text.strip(), cols[1].text.strip()
            stats_all[k] = v
            if k in ["Market Cap", "Revenue", "Employees"]:
                stats_selected[k] = v
    data["statistics_full"]     = stats_all
    data["statistics_selected"] = stats_selected

    driver.get(base_url + "history/")
    time.sleep(5)
    hist_all = {}
    for row in driver.find_elements(By.TAG_NAME, "tr"):
        cols = row.find_elements(By.TAG_NAME, "td")
        if len(cols) == 2:
            hist_all[cols[0].text.strip()] = cols[1].text.strip()
    data["history_full"] = hist_all

    driver.get(base_url + "company/")
    time.sleep(5)
    try:
        data["profile"] = driver.find_element(By.TAG_NAME, "main").text
    except:
        data["profile"] = driver.find_element(By.TAG_NAME, "body").text

    return data


# ─── 2. PURE PYTHON PARSER ────────────────────────────────────────────────────
#
# No LLM needed. The scraped data is consistent enough to parse directly.
# Each helper below targets a specific part of the raw dict.

def _clean_num(val: str) -> Optional[str]:
    """Strip commas, currency symbols, spaces. Return None if empty/na."""
    if not val:
        return None
    val = val.strip().replace(",", "").replace(" ", "")
    if val in ("n/a", "-", "—", "N/A", "", "NotFound", "Not Found"):
        return None
    return val


def _f(val) -> Optional[float]:
    """Parse a value to float. Accepts strings or numbers."""
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    cleaned = _clean_num(str(val))
    if cleaned is None:
        return None
    cleaned = cleaned.rstrip("%")
    try:
        return float(cleaned)
    except ValueError:
        return None


def _pct(val) -> Optional[float]:
    """Parse a percentage string to decimal fraction. '25.88%' → 0.2588"""
    raw = _f(val)
    if raw is None:
        return None
    s = str(val).strip()
    if "%" in s:
        return raw / 100.0
    if abs(raw) <= 1.5:
        return raw
    return raw / 100.0


def _i(val) -> Optional[int]:
    v = _f(val)
    return int(v) if v is not None else None


def _dec(val) -> Optional[Decimal]:
    v = _f(val)
    return Decimal(str(v)) if v is not None else None


def _parse_date(val: Optional[str]) -> Optional[date]:
    if not val:
        return None
    val = val.strip()
    for fmt in ("%Y-%m-%d", "%b %d, %Y", "%B %d, %Y", "%b %Y", "%B %Y"):
        try:
            return datetime.strptime(val, fmt).date()
        except (ValueError, TypeError):
            pass
    return None


def _strip_change(val: str) -> str:
    """'22.21T +127.5%' → '22.21T'"""
    if not val:
        return val
    return val.split()[0]


def _parse_scale(val: str) -> Optional[float]:
    """
    Parse values with T/B/M suffix into plain millions.
    '22.21T' → 22_210_000.0
    '8.68B'  →      8_680.0
    '500M'   →        500.0
    '1,234'  →      1_234.0  (already in millions per source)
    """
    if not val:
        return None
    val = _strip_change(str(val)).replace(",", "").strip()
    if val in ("n/a", "-", "—", "N/A", ""):
        return None
    multipliers = {"T": 1_000_000, "B": 1_000, "M": 1}
    for suffix, mult in multipliers.items():
        if val.upper().endswith(suffix):
            try:
                return float(val[:-1]) * mult
            except ValueError:
                return None
    try:
        return float(val)
    except ValueError:
        return None


def _parse_shares_raw(val: str) -> Optional[int]:
    """Source reports shares in millions — convert to raw count."""
    v = _f(val)
    if v is None:
        return None
    return int(v * 1_000_000)


# ── Financial table text parser ────────────────────────────────────────────────

def _parse_fin_table(text: str) -> dict:
    """
    Parse a multi-column financial table (scraped as plain text) into:
      { row_label: { period_key: raw_value_string, ... }, ... }

    The scraped table text looks like:
      Fiscal Year
      TTM FY 2025 FY 2024 FY 2023 ...
      Period Ending
      Dec 31, 2025 Mar 31, 2025 Mar 31, 2024 ...
      Revenue
      6,012 4,977 5,000 ...
      Revenue Growth (YoY)
      25.88% -0.46% -5.09% ...
    """
    if not text or text.strip() in ("Not Found", ""):
        return {}

    lines = [l.strip() for l in text.strip().splitlines() if l.strip()]

    # ── Step 1: find the column header line (contains TTM / FY / Current) ──
    header_idx = None
    for i, line in enumerate(lines):
        if re.search(r"\bTTM\b|\bFY\b|\bCurrent\b|\bQ[1-4]\b", line):
            header_idx = i
            break
    if header_idx is None:
        return {}

    # ── Step 2: build period labels ──────────────────────────────────────────
    # Raw header might be: "TTM FY 2025 FY 2024 FY 2023 FY 2022 FY 2021"
    tokens = lines[header_idx].split()
    period_labels = []
    i = 0
    while i < len(tokens):
        if tokens[i] == "FY" and i + 1 < len(tokens):
            period_labels.append(f"FY {tokens[i+1]}")
            i += 2
        else:
            period_labels.append(tokens[i])
            i += 1

    # ── Step 3: find actual period-ending dates ───────────────────────────────
    # They appear right after the "Period Ending" label line
    period_dates = list(period_labels)  # fallback to labels if dates not found
    for j in range(header_idx + 1, min(header_idx + 6, len(lines))):
        if "period ending" in lines[j].lower():
            if j + 1 < len(lines):
                raw_dates = lines[j + 1]
                found = re.findall(r"[A-Za-z]{3}\s+\d{1,2},\s*\d{4}", raw_dates)
                if found:
                    period_dates = found
            break

    num_cols = len(period_dates)

    # ── Step 4: skip past header block to data rows ───────────────────────────
    data_start = header_idx + 1
    for j in range(data_start, min(data_start + 6, len(lines))):
        # The date line looks like "Dec 31, 2025 Mar 31, 2025 ..."
        if re.search(r"[A-Za-z]{3}\s+\d{1,2},\s*\d{4}", lines[j]):
            data_start = j + 1
            break

    # ── Step 5: parse label/value row pairs ───────────────────────────────────
    result = {}
    k = data_start
    while k < len(lines):
        label = lines[k]

        # Skip lines that are themselves numbers (value rows without a label)
        if re.match(r"^[\d\-\+\.,% ]+$", label):
            k += 1
            continue

        # Look ahead for a value line
        if k + 1 < len(lines):
            value_line = lines[k + 1]
            values = value_line.split()
            # Value line must have at least one numeric token
            if values and re.search(r"[\d\-\+]", value_line) and len(values) <= num_cols + 2:
                row_data = {}
                for idx, pdate in enumerate(period_dates):
                    row_data[pdate] = values[idx] if idx < len(values) else None
                result[label] = row_data
                k += 2
                continue
        k += 1

    return result


def _get(table: dict, row_key: str, period: str, as_pct: bool = False) -> Optional[float]:
    """
    Look up (row_key, period) in a parsed table.
    Uses case-insensitive partial matching for robustness against
    minor label variations across different stocks.
    """
    row = table.get(row_key)
    if row is None:
        rk_lower = row_key.lower()
        for k in table:
            if rk_lower in k.lower():
                row = table[k]
                break
    if row is None:
        return None

    val = row.get(period)
    if val is None:
        for p_key in row:
            if period in p_key or p_key in period:
                val = row[p_key]
                break
    if val is None:
        return None

    return _pct(val) if as_pct else _f(val)


def _get_periods(table: dict) -> list[tuple[str, str]]:
    """Return [(period_date_str, period_type), ...] for all columns."""
    if not table:
        return []
    sample = next(iter(table.values()))
    result = []
    for p in sample.keys():
        if p == "TTM":
            result.append((p, "TTM"))
        elif p.startswith("Current"):
            result.append((p, "current"))
        else:
            result.append((p, "FY"))
    return result


# ── Profile parser ─────────────────────────────────────────────────────────────
#
# The profile page renders fields as "Label Value" on a SINGLE line, e.g.:
#   "Country United Kingdom"
#   "Founded 2018"
#   "Industry Radiotelephone Communications"
#   "Employees 4,381"
# We match these with known prefixes rather than a key→next-line approach.

# Known inline labels and what field they map to.
# Order matters — longer/more specific patterns first.
_PROFILE_FIELD_PATTERNS = [
    ("country",             r"^Country\s+(.+)$"),
    ("founded",             r"^Founded\s+(\d{4})"),
    ("industry",            r"^Industry\s+(.+)$"),
    ("employees",           r"^Employees\s+([\d,]+)"),
    ("website",             r"^Website\s+(\S+)"),
    ("sic_code",            r"^SIC Code\s+(\d+)"),
    ("fiscal_year_end",     r"^Fiscal Year\s+(.+)$"),
    ("exchange",            r"^Exchange\s+(.+)$"),
    ("reporting_currency",  r"^Reporting Currency\s+(\w+)"),
    ("sector",              r"^Sector\s+(.+)$"),
]

# CEO: appears in the Key Executives table as "Name  Position" rows.
# e.g. "Sunil Taldar Chief Executive Officer"
_CEO_PATTERN = re.compile(
    r"^(.+?)\s+Chief Executive Officer", re.IGNORECASE
)


def _parse_profile(profile_text: str, symbol: str) -> dict:
    result = {"symbol": symbol}
    lines = [l.strip() for l in profile_text.splitlines() if l.strip()]

    # ── Company name ──────────────────────────────────────────────────────────
    # Usually the first line that contains a legal suffix or the symbol
    for line in lines[:15]:
        if symbol.upper() in line.upper() or any(
            x in line for x in ["Plc", "Ltd", "Limited", "PLC", "Inc", "Corp", "Group"]
        ):
            name = re.sub(r"\s*\(.*?\)", "", line).strip()  # strip "(NGX:AIRTELAFRI)"
            if name and len(name) > 2:
                result["name"] = name
                break

    # ── Inline key-value fields ───────────────────────────────────────────────
    for line in lines:
        for field, pattern in _PROFILE_FIELD_PATTERNS:
            if field not in result:   # don't overwrite once found
                m = re.match(pattern, line, re.IGNORECASE)
                if m:
                    result[field] = m.group(1).strip()
                    break

        # CEO from Key Executives block
        if "ceo" not in result:
            m = _CEO_PATTERN.match(line)
            if m:
                result["ceo"] = m.group(1).strip()

    # ── Currency from header e.g. "Currency is NGN" ───────────────────────────
    m = re.search(r"[Cc]urrency is (\w{2,4})", profile_text)
    result["currency"] = m.group(1).upper() if m else None

    # ── Description: long paragraph after "Company Description" heading ───────
    for i, line in enumerate(lines):
        if "company description" in line.lower() or "about" == line.lower():
            # Grab the next few long lines (actual description sentences)
            desc_lines = [l for l in lines[i + 1: i + 8] if len(l) > 40]
            result["description"] = " ".join(desc_lines[:3]) or None
            break

    # ── Employees: convert "4,381" → int ─────────────────────────────────────
    if result.get("employees"):
        result["employees"] = _i(_clean_num(str(result["employees"])))

    return result


# ── Price / overview parser ────────────────────────────────────────────────────

def _parse_price(overview: dict, stats: dict, symbol: str) -> dict:
    def _ov(*keys):
        for k in keys:
            v = overview.get(k) or stats.get(k)
            if v:
                return v
        return None

    # 52-week range: "2,062.24 - 2,372.50"
    week_high = week_low = None
    rng = _ov("52-Week Range", "52 Week Range")
    if rng:
        parts = re.split(r"\s*[-–]\s*", rng)
        if len(parts) == 2:
            week_low  = _f(parts[0].replace(",", ""))
            week_high = _f(parts[1].replace(",", ""))

    # Day's range: "2,200.00 - 2,300.00"
    day_high = day_low = None
    drng = _ov("Day's Range", "Days Range")
    if drng:
        parts = re.split(r"\s*[-–]\s*", drng)
        if len(parts) == 2:
            day_low  = _f(parts[0].replace(",", ""))
            day_high = _f(parts[1].replace(",", ""))

    # Dividend: "102.49 (4.52%)"
    div_ps = div_yield = None
    div_raw = _ov("Dividend", "Dividend Yield")
    if div_raw:
        m = re.match(r"([\d,.]+)\s*\(([\d.]+)%\)", div_raw)
        if m:
            div_ps    = _f(m.group(1))
            div_yield = float(m.group(2)) / 100
        else:
            div_yield = _pct(div_raw)

    # Price date — try to extract from profile or fall back to today
    price_date = date.today().isoformat()

    return {
        "price_date":         price_date,
        "close_price":        _f(_strip_change(_ov("Previous Close", "Price") or "")),
        "open_price":         _f(_strip_change(_ov("Open") or "")),
        "day_high":           day_high,
        "day_low":            day_low,
        "week_52_high":       week_high,
        "week_52_low":        week_low,
        "volume":             _i(_clean_num(_ov("Volume"))),
        "avg_volume_20d":     _i(_clean_num(_ov("Average Volume", "Avg Volume (20 Days)"))),
        "rsi":                _f(_ov("RSI", "Relative Strength Index (RSI)")),
        "ma_50d":             _f(_clean_num(_ov("50-Day Moving Average"))),
        "ma_200d":            _f(_clean_num(_ov("200-Day Moving Average"))),
        "beta":               _f(_ov("Beta", "Beta (5Y)")),
        "market_cap":         _parse_scale(_ov("Market Cap")),
        "enterprise_value":   _parse_scale(_ov("Enterprise Value")),
        "pe_ratio":           _f(_ov("PE Ratio", "P/E Ratio")),
        "forward_pe":         _f(_ov("Forward PE", "Forward P/E")),
        "ps_ratio":           _f(_ov("PS Ratio", "P/S Ratio")),
        "pb_ratio":           _f(_ov("PB Ratio", "P/B Ratio")),
        "dividend_per_share": div_ps,
        "dividend_yield":     div_yield,
        "ex_dividend_date":   _parse_date(_ov("Ex-Dividend Date", "Ex-Div Date")).__str__()
                              if _parse_date(_ov("Ex-Dividend Date", "Ex-Div Date")) else None,
    }


# ── Section parsers ────────────────────────────────────────────────────────────

def _parse_income_periods(raw_data: dict) -> list[dict]:
    text  = raw_data.get("financials", {}).get("sections", {}).get("Income Statement", "")
    table = _parse_fin_table(text)
    rows  = []
    for pdate, ptype in _get_periods(table):
        g  = lambda key, pct=False, _p=pdate: _get(table, key, _p, as_pct=pct)
        pd = _parse_date(pdate)
        if not pd:
            continue
        rows.append({
            "period_ending":         pd.isoformat(),
            "period_type":           ptype,
            "revenue":               g("Revenue"),
            "operating_revenue":     g("Operating Revenue"),
            "other_revenue":         g("Other Revenue"),
            "revenue_growth_yoy":    g("Revenue Growth", pct=True),
            "cost_of_revenue":       g("Cost of Revenue"),
            "gross_profit":          g("Gross Profit"),
            "sga_expenses":          g("Selling, General"),
            "other_opex":            g("Other Operating Expenses"),
            "total_opex":            g("Operating Expenses"),
            "operating_income":      g("Operating Income"),
            "ebitda":                g("EBITDA"),
            "ebit":                  g("EBIT"),
            "interest_expense":      g("Interest Expense"),
            "pretax_income":         g("Pretax Income"),
            "income_tax":            g("Income Tax Expense"),
            "net_income":            g("Net Income"),
            "net_income_growth_yoy": g("Net Income Growth", pct=True),
            "minority_interest":     g("Minority Interest"),
            "eps_basic":             g("EPS (Basic)"),
            "eps_diluted":           g("EPS (Diluted)"),
            "eps_growth_yoy":        g("EPS Growth", pct=True),
            "dividend_per_share":    g("Dividend Per Share"),
            "dividend_growth_yoy":   g("Dividend Growth", pct=True),
            "shares_basic":          _parse_shares_raw(str(g("Shares Outstanding (Basic)") or "")),
            "shares_diluted":        _parse_shares_raw(str(g("Shares Outstanding (Diluted)") or "")),
            "shares_change_yoy":     g("Shares Change", pct=True),
            "gross_margin":          g("Gross Margin", pct=True),
            "operating_margin":      g("Operating Margin", pct=True),
            "profit_margin":         g("Profit Margin", pct=True),
            "ebitda_margin":         g("EBITDA Margin", pct=True),
            "effective_tax_rate":    g("Effective Tax Rate", pct=True),
            "free_cash_flow":        g("Free Cash Flow"),
            "fcf_per_share":         g("Free Cash Flow Per Share"),
            "fcf_margin":            g("Free Cash Flow Margin", pct=True),
        })
    return rows


def _parse_balance_periods(raw_data: dict) -> list[dict]:
    text  = raw_data.get("financials", {}).get("sections", {}).get("Balance Sheet", "")
    table = _parse_fin_table(text)
    rows  = []
    for pdate, ptype in _get_periods(table):
        g  = lambda key, _p=pdate: _get(table, key, _p)
        pd = _parse_date(pdate)
        if not pd:
            continue
        rows.append({
            "period_ending":             pd.isoformat(),
            "period_type":               ptype,
            "cash_equivalents":          g("Cash & Equivalents"),
            "short_term_investments":    g("Short-Term Investments"),
            "cash_and_st_investments":   g("Cash & Short-Term Investments"),
            "accounts_receivable":       g("Accounts Receivable"),
            "inventory":                 g("Inventory"),
            "restricted_cash":           g("Restricted Cash"),
            "other_current_assets":      g("Other Current Assets"),
            "total_current_assets":      g("Total Current Assets"),
            "ppe":                       g("Property, Plant"),
            "goodwill":                  g("Goodwill"),
            "intangible_assets":         g("Other Intangible Assets"),
            "long_term_investments":     g("Long-Term Investments"),
            "total_assets":              g("Total Assets"),
            "accounts_payable":          g("Accounts Payable"),
            "short_term_debt":           g("Short-Term Debt"),
            "current_ltdebt":            g("Current Portion of Long-Term Debt"),
            "current_leases":            g("Current Portion of Leases"),
            "unearned_revenue_current":  g("Current Unearned Revenue"),
            "total_current_liabilities": g("Total Current Liabilities"),
            "long_term_debt":            g("Long-Term Debt"),
            "long_term_leases":          g("Long-Term Leases"),
            "total_liabilities":         g("Total Liabilities"),
            "common_stock":              g("Common Stock"),
            "retained_earnings":         g("Retained Earnings"),
            "total_common_equity":       g("Total Common Equity"),
            "minority_interest":         g("Minority Interest"),
            "shareholders_equity":       g("Shareholders' Equity"),
            "total_debt":                g("Total Debt"),
            "net_cash_debt":             g("Net Cash"),
            "net_cash_per_share":        g("Net Cash Per Share"),
            "working_capital":           g("Working Capital"),
            "book_value_per_share":      g("Book Value Per Share"),
            "tangible_book_value":       g("Tangible Book Value"),
            "tangible_bvps":             g("Tangible Book Value Per Share"),
            "shares_outstanding":        _parse_shares_raw(str(g("Total Common Shares Outstanding") or "")),
        })
    return rows


def _parse_cashflow_periods(raw_data: dict) -> list[dict]:
    text  = raw_data.get("financials", {}).get("sections", {}).get("Cash Flow", "")
    table = _parse_fin_table(text)
    rows  = []
    for pdate, ptype in _get_periods(table):
        g  = lambda key, pct=False, _p=pdate: _get(table, key, _p, as_pct=pct)
        pd = _parse_date(pdate)
        if not pd:
            continue
        rows.append({
            "period_ending":             pd.isoformat(),
            "period_type":               ptype,
            "net_income":                g("Net Income"),
            "depreciation_amortization": g("Depreciation & Amortization"),
            "operating_cash_flow":       g("Operating Cash Flow"),
            "ocf_growth_yoy":            g("Operating Cash Flow Growth", pct=True),
            "capex":                     g("Capital Expenditures"),
            "sale_purchase_intangibles": g("Sale (Purchase) of Intangibles"),
            "investing_cash_flow":       g("Investing Cash Flow"),
            "debt_issued":               g("Total Debt Issued"),
            "debt_repaid":               g("Total Debt Repaid"),
            "net_debt_change":           g("Net Debt Issued"),
            "buybacks":                  g("Repurchase of Common Stock"),
            "dividends_paid":            g("Common Dividends Paid"),
            "financing_cash_flow":       g("Financing Cash Flow"),
            "net_cash_flow":             g("Net Cash Flow"),
            "free_cash_flow":            g("Free Cash Flow"),
            "fcf_growth_yoy":            g("Free Cash Flow Growth", pct=True),
            "fcf_margin":                g("Free Cash Flow Margin", pct=True),
            "fcf_per_share":             g("Free Cash Flow Per Share"),
            "levered_fcf":               g("Levered Free Cash Flow"),
            "unlevered_fcf":             g("Unlevered Free Cash Flow"),
            "cash_interest_paid":        g("Cash Interest Paid"),
            "cash_tax_paid":             g("Cash Income Tax Paid"),
            "change_in_working_capital": g("Change in Working Capital"),
        })
    return rows


def _parse_ratio_periods(raw_data: dict) -> list[dict]:
    text  = raw_data.get("financials", {}).get("sections", {}).get("Ratios", "")
    table = _parse_fin_table(text)
    rows  = []
    for pdate, ptype in _get_periods(table):
        g  = lambda key, pct=False, _p=pdate: _get(table, key, _p, as_pct=pct)
        gs = lambda key, _p=pdate: _parse_scale(str(_get(table, key, _p) or ""))
        pd = _parse_date(pdate)
        if not pd:
            continue
        rows.append({
            "period_ending":            pd.isoformat(),
            "period_type":              ptype,
            "market_cap":               gs("Market Capitalization"),
            "enterprise_value":         gs("Enterprise Value"),
            "market_cap_growth_yoy":    g("Market Cap Growth", pct=True),
            "last_close_price":         g("Last Close Price"),
            "pe_ratio":                 g("PE Ratio"),
            "ps_ratio":                 g("PS Ratio"),
            "pb_ratio":                 g("PB Ratio"),
            "p_fcf_ratio":              g("P/FCF Ratio"),
            "p_ocf_ratio":              g("P/OCF Ratio"),
            "ev_sales":                 g("EV/Sales"),
            "ev_ebitda":                g("EV/EBITDA"),
            "ev_ebit":                  g("EV/EBIT"),
            "ev_fcf":                   g("EV/FCF"),
            "debt_equity":              g("Debt / Equity"),
            "debt_ebitda":              g("Debt / EBITDA"),
            "debt_fcf":                 g("Debt / FCF"),
            "interest_coverage":        g("Interest Coverage"),
            "current_ratio":            g("Current Ratio"),
            "quick_ratio":              g("Quick Ratio"),
            "asset_turnover":           g("Asset Turnover"),
            "inventory_turnover":       g("Inventory Turnover"),
            "roe":                      g("Return on Equity", pct=True),
            "roa":                      g("Return on Assets", pct=True),
            "roic":                     g("Return on Invested Capital", pct=True),
            "roce":                     g("Return on Capital Employed", pct=True),
            "earnings_yield":           g("Earnings Yield", pct=True),
            "fcf_yield":                g("FCF Yield", pct=True),
            "dividend_yield":           g("Dividend Yield", pct=True),
            "buyback_yield":            g("Buyback Yield", pct=True),
            "total_shareholder_return": g("Total Shareholder Return", pct=True),
            "payout_ratio":             g("Payout Ratio", pct=True),
            "altman_z_score":           g("Altman Z-Score"),
            "piotroski_f_score":        _i(g("Piotroski F-Score")),
            "beta":                     g("Beta"),
        })
    return rows


# ── Main parse entry point ─────────────────────────────────────────────────────

def parse_raw_data(raw_data: dict, symbol: str) -> dict:
    """
    Pure Python deterministic parser. No LLM, no network calls.
    Returns the same structured dict format the upsert functions expect.
    """
    overview = raw_data.get("overview_full", {})
    stats    = raw_data.get("statistics_full", {})
    profile  = raw_data.get("profile", "")

    stock_info = _parse_profile(profile, symbol)

    # Fill gaps from overview/stats where profile text is ambiguous
    if not stock_info.get("exchange"):
        stock_info["exchange"] = overview.get("Exchange") or stats.get("Exchange")
    if not stock_info.get("employees"):
        stock_info["employees"] = _i(_clean_num(stats.get("Employees") or overview.get("Employees")))
    if not stock_info.get("industry"):
        stock_info["industry"] = stats.get("Industry") or overview.get("Industry")
    if not stock_info.get("sector"):
        stock_info["sector"] = stats.get("Sector") or overview.get("Sector")

    return {
        "stock":           stock_info,
        "stock_price":     _parse_price(overview, stats, symbol),
        "income_periods":  _parse_income_periods(raw_data),
        "balance_periods": _parse_balance_periods(raw_data),
        "cashflow_periods":_parse_cashflow_periods(raw_data),
        "ratio_periods":   _parse_ratio_periods(raw_data),
    }


# ─── 3. DB UPSERTS ────────────────────────────────────────────────────────────

def upsert_stock(session: Session, data: dict) -> Optional[Stock]:
    s = data.get("stock", {})
    symbol = s.get("symbol")
    if not symbol:
        print("⚠️  No symbol in parsed data.")
        return None
    stock = session.query(Stock).filter_by(symbol=symbol).first()
    if not stock:
        stock = Stock(symbol=symbol)
        session.add(stock)
    stock.name               = s.get("name")
    stock.exchange           = s.get("exchange")
    stock.currency           = s.get("currency")
    stock.reporting_currency = s.get("reporting_currency")
    stock.sector             = s.get("sector")
    stock.industry           = s.get("industry")
    stock.description        = s.get("description")
    stock.website            = s.get("website")
    stock.country            = s.get("country")
    stock.founded            = s.get("founded")
    stock.ceo                = s.get("ceo")
    stock.employees          = _i(s.get("employees"))
    stock.fiscal_year_end    = s.get("fiscal_year_end")
    stock.sic_code           = s.get("sic_code")
    stock.last_updated       = datetime.now(timezone.utc)
    session.flush()
    return stock


def upsert_price(session: Session, stock: Stock, data: dict):
    p = data.get("stock_price", {})
    price_date = p.get("price_date")
    if not price_date:
        return
    row = session.query(DailyKline).filter_by(symbol=stock.symbol, date=price_date).first()
    if not row:
        row = DailyKline(symbol=stock.symbol, date=price_date)
        session.add(row)
    if p.get("close_price")  is not None: row.close  = _f(p["close_price"])
    if p.get("open_price")   is not None: row.open   = _f(p["open_price"])
    if p.get("day_high")     is not None: row.high   = _f(p["day_high"])
    if p.get("day_low")      is not None: row.low    = _f(p["day_low"])
    if p.get("volume")       is not None: row.volume = _i(p["volume"])
    row.week_52_high       = _f(p.get("week_52_high"))
    row.week_52_low        = _f(p.get("week_52_low"))
    row.avg_volume_20d     = _i(p.get("avg_volume_20d"))
    row.rsi                = _f(p.get("rsi"))
    row.ma_50d             = _f(p.get("ma_50d"))
    row.ma_200d            = _f(p.get("ma_200d"))
    row.beta               = _f(p.get("beta"))
    row.market_cap         = _dec(p.get("market_cap"))
    row.enterprise_value   = _dec(p.get("enterprise_value"))
    row.pe_ratio           = _f(p.get("pe_ratio"))
    row.forward_pe         = _f(p.get("forward_pe"))
    row.ps_ratio           = _f(p.get("ps_ratio"))
    row.pb_ratio           = _f(p.get("pb_ratio"))
    row.dividend_per_share = _f(p.get("dividend_per_share"))
    row.dividend_yield     = _f(p.get("dividend_yield"))
    row.ex_dividend_date   = p.get("ex_dividend_date")
    row.adjustment_factor  = p.get("adjustment_factor")


def upsert_income_periods(session: Session, stock: Stock, data: dict):
    for period in data.get("income_periods", []):
        pe = _parse_date(period.get("period_ending"))
        pt = period.get("period_type")
        if not pe or not pt:
            continue
        row = session.query(IncomeStatement).filter_by(
            stock_id=stock.id, period_ending=pe, period_type=pt).first()
        if not row:
            row = IncomeStatement(stock_id=stock.id, period_ending=pe, period_type=pt)
            session.add(row)
        for f in ["revenue","operating_revenue","other_revenue","revenue_growth_yoy",
                  "cost_of_revenue","gross_profit","sga_expenses","other_opex","total_opex",
                  "operating_income","ebitda","ebit","interest_expense","pretax_income",
                  "income_tax","net_income","net_income_growth_yoy","minority_interest",
                  "eps_basic","eps_diluted","eps_growth_yoy","dividend_per_share",
                  "dividend_growth_yoy","shares_change_yoy","gross_margin","operating_margin",
                  "profit_margin","ebitda_margin","effective_tax_rate",
                  "free_cash_flow","fcf_per_share","fcf_margin"]:
            setattr(row, f, _f(period.get(f)))
        row.shares_basic   = _i(period.get("shares_basic"))
        row.shares_diluted = _i(period.get("shares_diluted"))


def upsert_balance_periods(session: Session, stock: Stock, data: dict):
    for period in data.get("balance_periods", []):
        pe = _parse_date(period.get("period_ending"))
        pt = period.get("period_type")
        if not pe or not pt:
            continue
        row = session.query(BalanceSheet).filter_by(
            stock_id=stock.id, period_ending=pe, period_type=pt).first()
        if not row:
            row = BalanceSheet(stock_id=stock.id, period_ending=pe, period_type=pt)
            session.add(row)
        for f in ["cash_equivalents","short_term_investments","cash_and_st_investments",
                  "accounts_receivable","inventory","restricted_cash","other_current_assets",
                  "total_current_assets","ppe","goodwill","intangible_assets",
                  "long_term_investments","total_assets","accounts_payable","short_term_debt",
                  "current_ltdebt","current_leases","unearned_revenue_current",
                  "total_current_liabilities","long_term_debt","long_term_leases",
                  "total_liabilities","common_stock","retained_earnings","total_common_equity",
                  "minority_interest","shareholders_equity","total_debt","net_cash_debt",
                  "net_cash_per_share","working_capital","book_value_per_share",
                  "tangible_book_value","tangible_bvps"]:
            setattr(row, f, _f(period.get(f)))
        row.shares_outstanding = _i(period.get("shares_outstanding"))


def upsert_cashflow_periods(session: Session, stock: Stock, data: dict):
    for period in data.get("cashflow_periods", []):
        pe = _parse_date(period.get("period_ending"))
        pt = period.get("period_type")
        if not pe or not pt:
            continue
        row = session.query(CashFlow).filter_by(
            stock_id=stock.id, period_ending=pe, period_type=pt).first()
        if not row:
            row = CashFlow(stock_id=stock.id, period_ending=pe, period_type=pt)
            session.add(row)
        for f in ["net_income","depreciation_amortization","operating_cash_flow","ocf_growth_yoy",
                  "capex","sale_purchase_intangibles","investing_cash_flow","debt_issued",
                  "debt_repaid","net_debt_change","buybacks","dividends_paid",
                  "financing_cash_flow","net_cash_flow","free_cash_flow","fcf_growth_yoy",
                  "fcf_margin","fcf_per_share","levered_fcf","unlevered_fcf",
                  "cash_interest_paid","cash_tax_paid","change_in_working_capital"]:
            setattr(row, f, _f(period.get(f)))


def upsert_ratio_periods(session: Session, stock: Stock, data: dict):
    for period in data.get("ratio_periods", []):
        pe = _parse_date(period.get("period_ending"))
        pt = period.get("period_type")
        if not pe or not pt:
            continue
        row = session.query(StockRatio).filter_by(
            stock_id=stock.id, period_ending=pe, period_type=pt).first()
        if not row:
            row = StockRatio(stock_id=stock.id, period_ending=pe, period_type=pt)
            session.add(row)
        for f in ["market_cap_growth_yoy","last_close_price","pe_ratio","ps_ratio","pb_ratio",
                  "p_fcf_ratio","p_ocf_ratio","ev_sales","ev_ebitda","ev_ebit","ev_fcf",
                  "debt_equity","debt_ebitda","debt_fcf","interest_coverage",
                  "current_ratio","quick_ratio","asset_turnover","inventory_turnover",
                  "roe","roa","roic","roce","earnings_yield","fcf_yield","dividend_yield",
                  "buyback_yield","total_shareholder_return","payout_ratio",
                  "altman_z_score","beta"]:
            setattr(row, f, _f(period.get(f)))
        row.market_cap        = _dec(period.get("market_cap"))
        row.enterprise_value  = _dec(period.get("enterprise_value"))
        row.piotroski_f_score = _i(period.get("piotroski_f_score"))


# ─── 4. PIPELINE ──────────────────────────────────────────────────────────────

def process_stock_data(raw_data: dict, symbol: str) -> None:
    print(f"⚙️  [{symbol}] Parsing...")
    parsed = parse_raw_data(raw_data, symbol)

    print(f"💾 [{symbol}] Writing to database...")
    with Session(engine) as session:
        with session.begin():
            stock = upsert_stock(session, parsed)
            if not stock:
                print(f"❌ [{symbol}] Skipping — no symbol.")
                return
            upsert_price(session, stock, parsed)
            upsert_income_periods(session, stock, parsed)
            upsert_balance_periods(session, stock, parsed)
            upsert_cashflow_periods(session, stock, parsed)
            upsert_ratio_periods(session, stock, parsed)

    print(f"✅ [{symbol}] Done.")


# ─── 5. ENTRY POINT ───────────────────────────────────────────────────────────

def make_driver():
    options = Options()
    options.add_argument("--headless")
    options.add_argument("--start-maximized")
    driver = webdriver.Chrome(options=options)
    driver.set_page_load_timeout(90)
    return driver, WebDriverWait(driver, 15)


if __name__ == "__main__":
    print("🗄️  Initializing database tables...")
    Base.metadata.create_all(bind=engine)

    driver, wait = make_driver()
    try:
        for i, symbol in enumerate(STOCK_CODES):
            if i > 0 and i % 10 == 0:
                print("♻️  Restarting driver...")
                try: driver.quit()
                except: pass
                driver, wait = make_driver()

            try:
                raw = scrape_stock_data(driver, wait, symbol)
                process_stock_data(raw, symbol)
                print(f"⏳ Waiting 12s...")
                time.sleep(12)
            except Exception as e:
                print(f"❌ [{symbol}] Error: {e}")
                err = str(e).lower()
                if any(x in err for x in ["httpconnectionpool", "read timed out", "invalid session id"]):
                    print("🔄 Restarting driver...")
                    try: driver.quit()
                    except: pass
                    driver, wait = make_driver()
                continue
    finally:
        try: driver.quit()
        except: pass
        print("🏁 Done.")