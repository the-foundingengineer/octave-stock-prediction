import csv
import os
import re

# Tickers from populate_stocks.py
STOCKS = [
    "ZICHIS", "ZENITHBANK", "WEMABANK", "VITAFOAM", "VFDGROUP", "VERITASKAP",
    "UPDCREIT", "UPDC", "UPL", "UNIVINSURE", "UNITYBNK", "UCAP", "UBA",
    "UNIONDICON", "UNILEVER", "UHOMREIT", "UACN", "TRIPPLEG", "TRANSCORP",
    "TRANSPOWER", "TRANSCOHOT", "TRANSEXPR", "TOTAL", "THOMASWY", "OKOMUOIL",
    "NGXGROUP", "TIP", "TANTALIZER", "SUNUASSUR", "STERLINGNG", "STANBIC",
    "STACO", "SOVRENINS", "SKYAVN", "SFSREIT", "SEPLAT", "NSLTECH", "SCOA",
    "ROYALEX", "RONCHESS", "REGALINS", "REDSTAREX", "RTBRISCOE", "PZ",
    "PRESTIGE", "PRESCO", "PREMPAINTS", "PHARMDEKO", "OMATEK", "OANDO",
    "NPFMCRFBK", "NNFM", "ENAMELWA", "NB", "NAHCO", "NESTLE", "NEM", "NEIMETH",
    "NCR", "NASCON", "MBENEFIT", "MULTIVERSE", "MULTITREX", "MTNN", "MORISON",
    "MEYER", "MECURE", "MCNICHOLS", "MAYBAKER", "LIVINGTRUST", "LIVESTOCK",
    "LINKASSURE", "LEGENDINT", "LEARNAFRCA", "LASACO", "WAPCO", "JBERGER",
    "JULI", "JOHNHOLT", "JAPAULGOLD", "JAIZBANK", "INTENEGINS", "INTBREW",
    "INFINITY", "IMG", "IKEJAHOTEL", "HONYFLOUR", "HMCALL", "GUINNESS",
    "GUINEAINS", "GTCO", "VANLEER", "GOLDBREW", "GEREGU", "FTNCOCOA",
    "FTGINSURE", "FIRSTHOLDCO", "FIDSON", "FIDELITYBK", "FCMB", "EUNISELL",
    "ETRANZACT", "ETERNA", "ELLAHLAKES", "EKOCORP", "ETI", "DUNLOP",
    "DEAPCAP", "DANGSUGAR", "DANGCEM", "DAARCOMM", "CWG", "CUTIX", "CUSTODIAN",
    "WAPIC", "CORNERST", "CONHALLPLC", "CONOIL", "CAP", "CHELLARAM", "CHAMS",
    "CHAMPION", "CAVERTON", "CADBURY", "CILEASING", "BUAFOODS", "BUACEMENT",
    "BAPLC", "BETAGLAS", "BERGER", "MANSARD", "AUSTINLAZ", "ARADEL", "ALEX",
    "AIRTELAFRI", "AIICO", "AFROMEDIA", "AFRINSURE", "AFRIPRUD", "ACCESSCORP",
    "ACADEMY", "ABCTRANS", "ABBEYBDS"
]

def clean_stock_name(name):
    if not name:
        return ""
    # remove "Stock Price History" (case insensitive)
    name = re.sub(r'Stock Price History', '', name, flags=re.IGNORECASE)
    # remove "//stock" (case insensitive)
    name = re.sub(r'//stock', '', name, flags=re.IGNORECASE)
    # remove "Stock\"
    name = name.replace("Stock\\", "")
    return name.strip()

def format_csv(input_file, output_file):
    print(f"Reading {input_file}...")
    
    formatted_rows = []
    
    # Pre-sort tickers by length descending to match longest possible ticker first
    sorted_tickers = sorted(STOCKS, key=len, reverse=True)
    
    with open(input_file, 'r', encoding='utf-8') as f:
        reader = csv.reader(f)
        
        for i, row in enumerate(reader):
            if not row or len(row) < 8:
                continue
                
            # Date, Price, Open, High, Low, Vol., Change %, Name, ...
            # Row 0 is header: ['Date', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %']
            # So data rows start at index 1 or later.
            
            date_str = row[0]
            # Check if it's a date
            if not re.match(r'\d{1,2}/\d{1,2}/\d{4}', date_str):
                continue
                
            raw_name = row[7]
            clean_name = clean_stock_name(raw_name)
            
            # Map ticker
            current_ticker = None
            # Try exact match first (case insensitive)
            for ticker in sorted_tickers:
                if ticker.upper() in clean_name.upper():
                    current_ticker = ticker
                    break
            
            if not current_ticker:
                # Try matching by first word or something
                words = clean_name.upper().split()
                if words:
                    for ticker in sorted_tickers:
                        if ticker.upper() in words:
                            current_ticker = ticker
                            break
            
            try:
                close = row[1]
                open_val = row[2]
                high = row[3]
                low = row[4]
                volume = row[5]
                
                formatted_rows.append({
                    'date': date_str,
                    'open': open_val,
                    'high': high,
                    'low': low,
                    'close': close,
                    'volume': volume,
                    'stock_name': clean_name,
                    'ticker': current_ticker or "UNKNOWN"
                })
            except IndexError:
                continue

    print(f"Writing {len(formatted_rows)} rows to {output_file}...")
    with open(output_file, 'w', encoding='utf-8', newline='') as f:
        fieldnames = ['date', 'open', 'high', 'low', 'close', 'volume', 'stock_name', 'ticker']
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(formatted_rows)
    print("Done.")

if __name__ == "__main__":
    format_csv('all_stocks.csv', 'formatted_stocks.csv')
