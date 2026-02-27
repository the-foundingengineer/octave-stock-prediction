import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time
import traceback
from sqlalchemy import desc
from app.database import SessionLocal
from app.models import Stock, DailyKline, Dividend, StockExecutive

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}

def parse_date(date_str):
    """Parse date strings like 'Jun 10, 2025' or 'Dec 31, 2024'."""
    if not date_str or date_str == "n/a":
        return None
    try:
        return datetime.strptime(date_str, "%b %d, %Y").date()
    except ValueError:
        try:
            return datetime.strptime(date_str, "%Y-%m-%d").date()
        except ValueError:
            return None

def parse_large_number(num_str):
    """
    Parse strings like '3.58T', '589.77B', '142.53M' to float.
    Handles 'T' (trillion), 'B' (billion), 'M' (million).
    """
    if not num_str or num_str == "n/a" or num_str == "-":
        return None
    
    num_str = num_str.replace(",", "").upper()
    multiplier = 1
    
    if num_str.endswith("T"):
        multiplier = 1_000_000_000_000
        num_str = num_str[:-1]
    elif num_str.endswith("B"):
        multiplier = 1_000_000_000
        num_str = num_str[:-1]
    elif num_str.endswith("M"):
        multiplier = 1_000_000
        num_str = num_str[:-1]
    elif num_str.endswith("K"):
        multiplier = 1_000
        num_str = num_str[:-1]
        
    try:
        return float(num_str) * multiplier
    except ValueError:
        return None

def parse_amount(amount_str):
    """Parse amount strings like '30.000 NGN' to (float, 'NGN')."""
    if not amount_str or amount_str == "n/a":
        return None, None
    parts = amount_str.split()
    try:
        amount = float(parts[0].replace(",", ""))
        currency = parts[1] if len(parts) > 1 else "NGN"
        return amount, currency
    except (ValueError, IndexError):
        # Could be a large number format like '3.58T'
        large = parse_large_number(amount_str)
        if large is not None:
            return large, "NGN"
        return None, None

def parse_percent(percent_str):
    """Parse percentage strings like '4.13%' or '52.62%' to float."""
    if not percent_str or percent_str == "n/a" or percent_str == "-":
        return None
    try:
        return float(percent_str.replace("%", "").replace(",", ""))
    except ValueError:
        return None

def scrape_dividend_data(symbol: str):
    """Scrapes dividend summary stats and history."""
    url = f"https://stockanalysis.com/quote/ngx/{symbol.upper()}/dividend/"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code == 404: return None
        res.raise_for_status()
    except: return None

    soup = BeautifulSoup(res.text, "html.parser")
    stats = {"dividend_yield": None, "annual_dividend": None, "ex_dividend_date": None, 
             "payout_frequency": None, "payout_ratio": None, "dividend_growth": None}
    
    # Improved stats scraping: Look for labels by text content
    mapping = {
        "Dividend Yield": "dividend_yield",
        "Annual Dividend": "annual_dividend",
        "Ex-Dividend Date": "ex_dividend_date",
        "Payout Frequency": "payout_frequency",
        "Payout Ratio": "payout_ratio",
        "Dividend Growth": "dividend_growth"
    }

    for label, key in mapping.items():
        label_node = soup.find(string=lambda t: t and label in t)
        if label_node:
            # Usually the value is in a sibling or the next parent-level container
            parent = label_node.parent
            # Often it's structured as: <div>Label</div><div>Value</div>
            # Or the label and value are in siblings
            container = parent.parent # The common container for label/value
            if container:
                text = container.get_text(separator="|", strip=True)
                parts = text.split("|")
                # Look for the part immediately after the label (or containing the value)
                for i, part in enumerate(parts):
                    if label in part:
                        if i + 1 < len(parts):
                            val = parts[i+1]
                            if key in ["dividend_yield", "payout_ratio", "dividend_growth"]:
                                stats[key] = parse_percent(val)
                            elif key == "annual_dividend":
                                stats[key], _ = parse_amount(val)
                            else:
                                stats[key] = val
                        break

    history = []
    # Fallback search for table if #main-table is missing
    table = soup.find("table", id="main-table") or soup.find("table")
    if table:
        tbody = table.find("tbody")
        rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:] # Skip header if no tbody
        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 4:
                ex_date = parse_date(cols[0])
                amount, currency = parse_amount(cols[1])
                if ex_date:
                    history.append({"ex_dividend_date": str(ex_date), "amount": amount, "currency": currency,
                                    "record_date": str(parse_date(cols[2])) if parse_date(cols[2]) else None, 
                                    "pay_date": str(parse_date(cols[3])) if parse_date(cols[3]) else None})
    return {"stats": stats, "history": history}

def scrape_revenue_data(symbol: str):
    """Scrapes revenue summary stats and history."""
    url = f"https://stockanalysis.com/quote/ngx/{symbol.upper()}/revenue/"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code == 404: return None
        res.raise_for_status()
    except: return None

    soup = BeautifulSoup(res.text, "html.parser")
    stats = {"revenue_ttm": None, "revenue_growth": None, "ps_ratio": None, "revenue_per_employee": None}
    
    mapping = {
        "Revenue (ttm)": "revenue_ttm",
        "Revenue Growth": "revenue_growth",
        "P/S Ratio": "ps_ratio",
        "Revenue / Employee": "revenue_per_employee"
    }

    for label, key in mapping.items():
        label_node = soup.find(string=lambda t: t and label in t)
        if label_node:
            parent = label_node.parent
            container = parent.parent
            if container:
                text = container.get_text(separator="|", strip=True)
                parts = text.split("|")
                for i, part in enumerate(parts):
                    if label in part:
                        if i + 1 < len(parts):
                            val = parts[i+1]
                            if key == "revenue_ttm" or key == "revenue_per_employee":
                                stats[key] = parse_large_number(val)
                            elif key == "revenue_growth":
                                stats[key] = parse_percent(val)
                            elif key == "ps_ratio":
                                try:
                                    stats[key] = float(val.replace(",", ""))
                                except ValueError:
                                    stats[key] = None
                        break

    history = []
    table = soup.find("table", id="main-table") or soup.find("table")
    if table:
        tbody = table.find("tbody")
        rows = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
        for row in rows:
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if len(cols) >= 4:
                fy_end = parse_date(cols[0])
                revenue = parse_large_number(cols[1])
                if fy_end:
                    history.append({
                        "fiscal_year_end": fy_end,
                        "revenue": revenue,
                        "change": parse_large_number(cols[2]),
                        "growth": parse_percent(cols[3])
                    })
    return {"stats": stats, "history": history}

def scrape_profile_data(symbol: str):
    """Scrapes company description, HQ, founded, and executives."""
    url = f"https://stockanalysis.com/quote/ngx/{symbol.upper()}/company/"
    try:
        res = requests.get(url, headers=HEADERS, timeout=10)
        if res.status_code == 404: return None
        res.raise_for_status()
    except: return None

    soup = BeautifulSoup(res.text, "html.parser")
    
    # Description
    desc_section = soup.find("div", class_="mt-2 text-sm text-gray-700") or soup.find("section", id="company-description")
    description = desc_section.get_text(strip=True, separator='\n') if desc_section else None

    # Profile Info Grid
    profile_info = {
        "headquarters": None,
        "founded": None,
        "employees": None,
        "website": None
    }
    
    # Look for the info list
    info_items = soup.find_all("div", class_="mb-4 lg:mb-0")
    for item in info_items:
        label_div = item.find("div", class_="text-sm font-bold text-gray-600 uppercase mb-1")
        val_div = item.find("div", class_="text-lg text-gray-900") or item.find("a")
        
        if label_div and val_div:
            label = label_div.get_text(strip=True)
            value = val_div.get_text(strip=True)
            
            if "Headquartered" in label: profile_info["headquarters"] = value
            elif "Founded" in label: profile_info["founded"] = value
            elif "Employees" in label: profile_info["employees"] = int(value.replace(",", "")) if value.replace(",", "").isdigit() else None
            elif "Website" in label: profile_info["website"] = val_div.get("href") or value

    # Executives
    executives = []
    exec_table = soup.find("table", id="executives-table") or soup.find("table")
    # Small heuristic: if we already found a table for statistics or history, the executives table is likely another one
    # But usually on the /company/ page, there is only one major table for executives
    if exec_table:
        tbody = exec_table.find("tbody")
        if tbody:
            for row in tbody.find_all("tr"):
                cols = [c.get_text(strip=True) for c in row.find_all("td")]
                if len(cols) >= 2:
                    name = cols[0]
                    title = cols[1]
                    age = int(cols[2]) if len(cols) > 2 and cols[2].isdigit() else None
                    since = cols[3] if len(cols) > 3 else None
                    executives.append({"name": name, "title": title, "age": age, "since": since})

    return {
        "description": description,
        "profile": profile_info,
        "executives": executives
    }

def populate_stock_financials():
    print(f"Starting financials population at {datetime.now()}...", flush=True)
    try:
        # Get list of symbols first using a temporary session
        temp_db = SessionLocal()
        stocks = temp_db.query(Stock).all()
        stock_data = [{"id": s.id, "symbol": s.symbol} for s in stocks]
        temp_db.close()
        
        print(f"Processing financials for {len(stock_data)} stocks...")
        
        for stock_info in stock_data:
            db = SessionLocal()
            try:
                symbol = stock_info["symbol"]
                stock_id = stock_info["id"]
                stock = db.query(Stock).get(stock_id)
                
                print(f"\n--- {symbol} ---", flush=True)
                
                # ── Scrape & Update Dividends ──────────────────────────────────
                div_data = scrape_dividend_data(symbol)
                if div_data:
                    stats = div_data["stats"]
                    print(f"  Scraped dividends: {len(div_data['history'])} rows", flush=True)

                    for item in div_data["history"]:
                        ex_date_str = str(item["ex_dividend_date"])
                        existing = db.query(Dividend).filter(Dividend.stock_id == stock_id, Dividend.ex_dividend_date == ex_date_str).first()
                        if not existing:
                            db.add(Dividend(
                                stock_id=stock_id, 
                                ex_dividend_date=ex_date_str,
                                record_date=str(item["record_date"]) if item["record_date"] else None,
                                pay_date=str(item["pay_date"]) if item["pay_date"] else None,
                                amount=item["amount"],
                                currency=item["currency"],
                                frequency=stats["payout_frequency"]
                            ))
                        else:
                            existing.amount, existing.currency, existing.frequency = item["amount"], item["currency"], stats["payout_frequency"]
                            existing.record_date = str(item["record_date"]) if item["record_date"] else None
                            existing.pay_date = str(item["pay_date"]) if item["pay_date"] else None
                
                # ── Scrape & Update Revenue ───────────────────────────────────
                # Revenue history is now served from income_statements.
                # The scrape_revenue_data function is kept for reference but
                # no longer writes to the database.

                db.commit()
                print(f"  Successfully committed {symbol}", flush=True)
                time.sleep(1) # Be nice
                
            except Exception as e:
                print(f"  Error processing {stock_info['symbol']}: {e}", flush=True)
                db.rollback()
            finally:
                db.close()
                
    except Exception as e:
        print(f"Fatal error in populate_stock_financials: {e}", flush=True)
        traceback.print_exc()

if __name__ == "__main__":
    populate_stock_financials()
