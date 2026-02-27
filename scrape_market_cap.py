"""
Scrape current market capitalization for all stocks from stockanalysis.com
and store in the market_cap_history table.

Usage:
    # Scrape all stocks:
    python scrape_market_cap.py

    # Scrape specific symbols:
    python -c "from scrape_market_cap import scrape_and_store_market_cap; scrape_and_store_market_cap(symbols=['DANGCEM','BUACEMENT'])"
"""

import re
import time
import traceback
from datetime import date, datetime

import requests
from bs4 import BeautifulSoup

from app.database import SessionLocal
from app.models import MarketCapHistory, Stock


HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
}


def parse_market_cap(text: str):
    """
    Parse market cap text like '13.40T', '589.77B', '142.53M' to a raw number.
    Returns float or None.
    """
    if not text:
        return None
    text = text.strip().replace(",", "").upper()
    multipliers = {"T": 1_000_000_000_000, "B": 1_000_000_000, "M": 1_000_000, "K": 1_000}
    for suffix, mult in multipliers.items():
        if text.endswith(suffix):
            try:
                return float(text[:-1]) * mult
            except ValueError:
                return None
    try:
        return float(text)
    except ValueError:
        return None


def scrape_current_market_cap(symbol: str):
    """
    Scrape the current market cap from the stock overview page.
    Returns (market_cap_float, date_str) or (None, None).
    """
    url = f"https://stockanalysis.com/quote/ngx/{symbol.upper()}/"
    try:
        res = requests.get(url, headers=HEADERS, timeout=15)
        if res.status_code == 404:
            return None, None
        res.raise_for_status()
    except Exception:
        return None, None

    soup = BeautifulSoup(res.text, "html.parser")

    # Strategy 1: Look for "Market Cap" label in table rows
    for row in soup.find_all("tr"):
        cols = row.find_all("td")
        if len(cols) == 2:
            label = cols[0].get_text(strip=True)
            if "Market Cap" in label:
                raw_val = cols[1].get_text(strip=True)
                # Strip trailing change info: "13.40T +5.2%" → "13.40T"
                raw_val = raw_val.split()[0] if raw_val else ""
                mcap = parse_market_cap(raw_val)
                if mcap:
                    return mcap, date.today().isoformat()

    # Strategy 2: Look for text containing "market cap" in stat blocks
    for label_node in soup.find_all(string=lambda t: t and "Market Cap" in t):
        parent = label_node.parent
        if parent:
            container = parent.parent
            if container:
                text = container.get_text(separator="|", strip=True)
                parts = text.split("|")
                for i, part in enumerate(parts):
                    if "Market Cap" in part and i + 1 < len(parts):
                        raw_val = parts[i + 1].split()[0]
                        mcap = parse_market_cap(raw_val)
                        if mcap:
                            return mcap, date.today().isoformat()

    return None, None


def scrape_and_store_market_cap(symbols=None):
    """
    Scrape and store current market cap for stocks.
    If symbols is None, processes all stocks in the database.
    """
    print(f"[{datetime.now()}] Starting market cap scrape...")

    db = SessionLocal()
    try:
        if symbols:
            stocks = db.query(Stock).filter(Stock.symbol.in_([s.upper() for s in symbols])).all()
        else:
            stocks = db.query(Stock).all()

        print(f"  Processing {len(stocks)} stocks...")

        success = 0
        failed = 0
        skipped = 0

        for stock in stocks:
            try:
                mcap, date_str = scrape_current_market_cap(stock.symbol)

                if mcap is None:
                    print(f"  ⚠️  {stock.symbol}: No market cap found")
                    failed += 1
                    continue

                # Check if we already have today's record
                existing = (
                    db.query(MarketCapHistory)
                    .filter(
                        MarketCapHistory.stock_id == stock.id,
                        MarketCapHistory.date == date_str,
                    )
                    .first()
                )

                if existing:
                    existing.market_cap = mcap
                    existing.frequency = "daily"
                    skipped += 1
                    print(f"  🔄 {stock.symbol}: Updated existing record → {mcap:,.0f}")
                else:
                    db.add(MarketCapHistory(
                        stock_id=stock.id,
                        date=date_str,
                        market_cap=mcap,
                        frequency="daily",
                    ))
                    success += 1
                    print(f"  ✅ {stock.symbol}: {mcap:,.0f}")

                db.commit()
                time.sleep(1)  # Be nice to the server

            except Exception as e:
                print(f"  ❌ {stock.symbol}: Error - {e}")
                traceback.print_exc()
                db.rollback()
                failed += 1

        print(f"\n🏁 Scrape complete. Inserted: {success}, Updated: {skipped}, Failed: {failed}")

    finally:
        db.close()


if __name__ == "__main__":
    scrape_and_store_market_cap()
