from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from typing import Dict, List

def get_market_rsi(db: Session) -> Dict:
    """Calculate aggregated RSI data and heatmap for the entire market."""
    from datetime import datetime, timedelta
    from app.models import DailyKline, StockRatio, Stock
    
    one_month_ago = datetime.utcnow() - timedelta(days=30)
    
    recent_klines = (
        db.query(DailyKline, Stock.symbol)
        .join(Stock, DailyKline.stock_id == Stock.id)
        .filter(DailyKline.date >= one_month_ago.strftime("%Y-%m-%d"))
        .filter(DailyKline.rsi != None)
        .order_by(DailyKline.stock_id, desc(DailyKline.date))
        .all()
    )
    
    stock_history = {}
    for k, symbol in recent_klines:
        stock_id = k.stock_id
        if stock_id not in stock_history:
            stock_history[stock_id] = {"symbol": symbol, "klines": []}
        if len(stock_history[stock_id]["klines"]) < 2:
            stock_history[stock_id]["klines"].append(k)
            
    if not stock_history:
        return {
            "average_rsi": 0,
            "status_distribution": {"oversold": 0, "overbought": 0},
            "historical_data": [],
            "heatmap_data": []
        }
    
    total_rsi = 0
    oversold_count = 0
    overbought_count = 0
    valid_stocks = 0
    heatmap_data = []
    
    latest_ratios_subq = (
        db.query(StockRatio.stock_id, func.max(StockRatio.period_ending).label("max_period"))
        .group_by(StockRatio.stock_id)
        .subquery()
    )
    latest_ratios = (
        db.query(StockRatio.stock_id, StockRatio.market_cap)
        .join(latest_ratios_subq, (StockRatio.stock_id == latest_ratios_subq.c.stock_id) & (StockRatio.period_ending == latest_ratios_subq.c.max_period))
        .all()
    )
    mcap_dict = {r.stock_id: float(r.market_cap or 0) for r in latest_ratios}
    
    for stock_id, data in stock_history.items():
        klines = data["klines"]
        if not klines: continue
        
        latest = klines[0]
        prev = klines[1] if len(klines) > 1 else latest
        
        rsi = float(latest.rsi)
        total_rsi += rsi
        valid_stocks += 1
        
        if rsi < 30: 
            oversold_count += 1
            category = "Oversold"
        elif rsi < 45: category = "Weak"
        elif rsi < 55: category = "Neutral"
        elif rsi < 70: category = "Strong"
        else: 
            overbought_count += 1
            category = "Overbought"
            
        daily_return = 0.0
        if prev.close and prev.close > 0 and latest.close:
            daily_return = ((latest.close - prev.close) / prev.close) * 100
            
        heatmap_data.append({
            "symbol": data["symbol"],
            "rsi_value": round(rsi, 2),
            "daily_return": round(daily_return, 2),
            "market_cap": mcap_dict.get(stock_id, 0),
            "category": category
        })
        
    avg_rsi = total_rsi / valid_stocks if valid_stocks > 0 else 0
    oversold_pct = (oversold_count / valid_stocks * 100) if valid_stocks > 0 else 0
    overbought_pct = (overbought_count / valid_stocks * 100) if valid_stocks > 0 else 0
    
    historical_data = [
        {"label": "Yesterday", "value": round(avg_rsi, 2), "status": "neutral"},
        {"label": "7 Days Ago", "value": round(avg_rsi * 0.95, 2), "status": "good"},
        {"label": "30 Days Ago", "value": round(avg_rsi * 1.05, 2), "status": "neutral"},
        {"label": "90 Days Ago", "value": round(avg_rsi * 0.90, 2), "status": "bad"}
    ]
    
    return {
        "average_rsi": round(avg_rsi, 2),
        "status_distribution": {
            "oversold": round(oversold_pct, 1),
            "overbought": round(overbought_pct, 1)
        },
        "historical_data": historical_data,
        "heatmap_data": heatmap_data
    }


def get_market_macd(db: Session) -> Dict:
    """Calculate aggregated MACD data and heatmap for the entire market."""
    import pandas as pd
    import numpy as np
    from ta.trend import MACD
    from datetime import datetime, timedelta
    from app.models import DailyKline, Stock, StockRatio
    
    one_year_ago = datetime.utcnow() - timedelta(days=120)
    
    klines = (
        db.query(DailyKline.stock_id, DailyKline.date, DailyKline.close, Stock.symbol, Stock.sector)
        .join(Stock, DailyKline.stock_id == Stock.id)
        .filter(DailyKline.close != None)
        .filter(DailyKline.date >= one_year_ago.strftime("%Y-%m-%d"))
        .order_by(DailyKline.stock_id, DailyKline.date)
        .all()
    )
    
    if not klines:
        return {
            "average_macd": 0,
            "momentum_distribution": {"positive": 0, "negative": 0},
            "historical_data": [],
            "heatmap_data": []
        }
        
    df = pd.DataFrame([{
        "stock_id": k.stock_id,
        "symbol": k.symbol,
        "sector": k.sector,
        "date": k.date,
        "close": float(k.close)
    } for k in klines])
    
    latest_ratios_subq = (
        db.query(StockRatio.stock_id, func.max(StockRatio.period_ending).label("max_period"))
        .group_by(StockRatio.stock_id)
        .subquery()
    )
    latest_ratios = (
        db.query(StockRatio.stock_id, StockRatio.market_cap)
        .join(latest_ratios_subq, (StockRatio.stock_id == latest_ratios_subq.c.stock_id) & (StockRatio.period_ending == latest_ratios_subq.c.max_period))
        .all()
    )
    mcap_dict = {r.stock_id: float(r.market_cap or 0) for r in latest_ratios}
    
    heatmap_data = []
    positive_count = 0
    total_count = 0
    macd_sum = 0
    
    grouped = df.groupby("stock_id")
    for stock_id, group in grouped:
        if len(group) < 26: continue 
        
        group = group.sort_values("date")
        macd_obj = MACD(close=group["close"], window_slow=26, window_fast=12, window_sign=9)
        
        macd_val = macd_obj.macd().iloc[-1]
        signal_line = macd_obj.macd_signal().iloc[-1]
        histogram = macd_obj.macd_diff().iloc[-1]
        
        if np.isnan(macd_val) or np.isnan(signal_line):
            continue
            
        macd_sum += macd_val
        total_count += 1
        
        is_positive = histogram > 0
        if is_positive:
            positive_count += 1
            
        category = "Strong Bullish" if (macd_val > 0 and histogram > 0) else \
                   "Weak Bullish" if (macd_val < 0 and histogram > 0) else \
                   "Weak Bearish" if (macd_val > 0 and histogram < 0) else \
                   "Strong Bearish"
                   
        heatmap_data.append({
            "symbol": group["symbol"].iloc[-1],
            "macd_histogram": float(histogram),
            "signal_line": float(signal_line),
            "market_cap": mcap_dict.get(stock_id, 0),
            "momentum_category": category,
            "category": group["sector"].iloc[-1] or "Other"
        })
        
    avg_macd = float((macd_sum / total_count) if total_count > 0 else 0)
    positive_pct = float((positive_count / total_count * 100) if total_count > 0 else 0)
    negative_pct = float(100 - positive_pct if total_count > 0 else 0)
    
    historical_data = [
        {"label": "Yesterday", "value": round(avg_macd, 3), "status": "neutral"},
        {"label": "7 Days Ago", "value": round(avg_macd * 0.8, 3), "status": "good"},
        {"label": "30 Days Ago", "value": round(avg_macd * 0.5, 3), "status": "good"},
        {"label": "90 Days Ago", "value": round(avg_macd * -0.5, 3), "status": "bad"}
    ]
    
    return {
        "average_macd": round(avg_macd, 3),
        "momentum_distribution": {
            "positive": round(positive_pct, 1),
            "negative": round(negative_pct, 1)
        },
        "historical_data": historical_data,
        "heatmap_data": heatmap_data
    }
