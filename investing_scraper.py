import time
import requests
from bs4 import BeautifulSoup
from datetime import datetime, date
from typing import Optional, List, Dict
from sqlalchemy.orm import Session
from app.database import engine, SessionLocal
from app.models import MarketIndex, MacroRate, Base

# Config
# -----------------------------------------------------------------------------
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://ng.investing.com/",
}

# NGX ASI Historical (curr_id=941199)
ASI_URL = "https://ng.investing.com/indices/nse-all-share-historical-data"
# Nigeria 10-Year Bond Yield
BOND_10Y_URL = "https://ng.investing.com/rates-bonds/nigeria-10-year-historical-data"

# Ensure tables are created
Base.metadata.create_all(engine)

def fetch(url: str) -> Optional[BeautifulSoup]:
    try:
        print(f"Fetching {url}...")
        response = requests.get(url, headers=HEADERS, timeout=15)
        if response.status_code != 200:
            print(f"Error: Status code {response.status_code}")
            return None
        print(f"Fetched {url} successfully.")
        return BeautifulSoup(response.text, "html.parser")
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None

def parse_investing_date(date_str: str) -> Optional[date]:
    """Handles formats like '02/20/2026' or 'Feb 20, 2026'"""
    for fmt in ("%m/%d/%Y", "%d/%m/%Y", "%b %d, %Y", "%B %d, %Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).date()
        except ValueError:
            continue
    return None

def parse_investing_number(num_str: str) -> Optional[float]:
    if not num_str:
        return None
    num_str = num_str.strip().replace(",", "").replace("%", "")
    if num_str in ("", "-", "—"):
        return None
    try:
        return float(num_str)
    except ValueError:
        return None

def scrape_historical_table(soup: BeautifulSoup):
    # Try finding the data-test attribute first
    table = soup.find("table", {"data-test": "historical-data-table"})
    if not table:
        # Fallback to general table search
        tables = soup.find_all("table")
        for t in tables:
            if "Date" in t.text and "Price" in t.text:
                table = t
                break
    
    if not table:
        print("Table not found on page.")
        return []

    rows = []
    thead = table.find("thead")
    if not thead:
        # Some tables don't have thead, try first row
        tr_headers = table.find("tr")
        if not tr_headers: return []
        headers = [th.text.strip().lower() for th in tr_headers.find_all(["th", "td"])]
    else:
        headers = [th.text.strip().lower() for th in thead.find_all("th")]
    
    print(f"Found table headers: {headers}")
    
    tbody = table.find("tbody")
    tr_list = tbody.find_all("tr") if tbody else table.find_all("tr")[1:]
        
    for tr in tr_list:
        cells = [td.text.strip() for td in tr.find_all("td")]
        if len(cells) < 2:
            continue
        row_data = dict(zip(headers, cells))
        rows.append(row_data)
    
    return rows

def sync_asi():
    print("\n--- Syncing ASI ---")
    # For ASI, we might want to try adding date params if the site supports them in GET
    # but for now we take the default first page (usually ~20-60 rows)
    soup = fetch(ASI_URL)
    if not soup:
        return
    
    rows = scrape_historical_table(soup)
    print(f"Found {len(rows)} rows of ASI data.")
    db = SessionLocal()
    try:
        added_count = 0
        for row in rows:
            dt = parse_investing_date(row.get("date"))
            if not dt:
                continue
            
            existing = db.query(MarketIndex).filter_by(symbol="NGSEINDEX", date=dt).first()
            if existing:
                continue
            
            price = parse_investing_number(row.get("price") or row.get("last"))
            if price is None:
                continue
                
            idx = MarketIndex(
                symbol="NGSEINDEX",
                name="NSE All Share Index",
                date=dt,
                price=price,
                open=parse_investing_number(row.get("open")),
                high=parse_investing_number(row.get("high")),
                low=parse_investing_number(row.get("low")),
                volume=int(parse_investing_number(row.get("vol.")) or 0),
                change_pct=parse_investing_number(row.get("change %"))
            )
            db.add(idx)
            added_count += 1
        db.commit()
        print(f"Added {added_count} new ASI records.")
    except Exception as e:
        print(f"DB Error ASI: {e}")
        db.rollback()
    finally:
        db.close()

def sync_bonds():
    print("\n--- Syncing 10-Year Bond Yield ---")
    soup = fetch(BOND_10Y_URL)
    if not soup:
        return
    
    rows = scrape_historical_table(soup)
    print(f"Found {len(rows)} rows of Bond data.")
    db = SessionLocal()
    try:
        added_count = 0
        for row in rows:
            dt = parse_investing_date(row.get("date"))
            if not dt:
                continue
            
            existing = db.query(MacroRate).filter_by(symbol="NGN_10Y_BOND", date=dt).first()
            if existing:
                continue
            
            val = parse_investing_number(row.get("price") or row.get("last"))
            if val is None:
                continue

            rate = MacroRate(
                symbol="NGN_10Y_BOND",
                name="Nigeria 10-Year Bond Yield",
                date=dt,
                value=val,
                unit="percentage"
            )
            db.add(rate)
            added_count += 1
        db.commit()
        print(f"Added {added_count} new 10Y Bond records.")
    except Exception as e:
        print(f"DB Error Bonds: {e}")
        db.rollback()
    finally:
        db.close()

if __name__ == "__main__":
    sync_asi()
    time.sleep(2)
    sync_bonds()
