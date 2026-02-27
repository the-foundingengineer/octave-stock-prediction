import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import desc
from typing import Dict, Any, List, Optional
from datetime import datetime

from app.models import DailyKline, Stock, AnalystForecast

# import ta technical indicators
from ta.trend import SMAIndicator, EMAIndicator, MACD, CCIIndicator
from ta.momentum import RSIIndicator, StochasticOscillator
from ta.volatility import AverageTrueRange

def _calculate_moving_averages(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculates SMAs and EMAs, returns signals and values."""
    periods = [5, 10, 20, 50, 100, 200]
    result = {"items": [], "summary": {"buy": 0, "sell": 0, "neutral": 0}}
    last_close = df['close'].iloc[-1]
    
    for p in periods:
        if len(df) >= p:
            # SMA
            sma_val = SMAIndicator(close=df['close'], window=p).sma_indicator().iloc[-1]
            if not np.isnan(sma_val):
                sma_sig = "Buy" if last_close > sma_val else "Sell"
                result["items"].append({"name": f"SMA {p}", "value": sma_val, "signal": sma_sig})
                if sma_sig == "Buy": result["summary"]["buy"] += 1
                else: result["summary"]["sell"] += 1
            
            # EMA
            ema_val = EMAIndicator(close=df['close'], window=p).ema_indicator().iloc[-1]
            if not np.isnan(ema_val):
                ema_sig = "Buy" if last_close > ema_val else "Sell"
                result["items"].append({"name": f"EMA {p}", "value": ema_val, "signal": ema_sig})
                if ema_sig == "Buy": result["summary"]["buy"] += 1
                else: result["summary"]["sell"] += 1

    return result

def _calculate_oscillators(df: pd.DataFrame) -> Dict[str, Any]:
    """Calculates Oscillators (RSI, Stochastic, MACD, etc), returns signals and values."""
    result = {"items": [], "summary": {"buy": 0, "sell": 0, "neutral": 0}}
    
    if len(df) < 14:
        return result

    # 1. RSI (14)
    rsi = RSIIndicator(close=df['close'], window=14).rsi().iloc[-1]
    if not np.isnan(rsi):
        sig = "Buy" if rsi < 30 else ("Sell" if rsi > 70 else "Neutral")
        result["items"].append({"name": "RSI (14)", "value": rsi, "signal": sig})
        result["summary"][sig.lower()] += 1

    # 2. MACD (12, 26)
    if len(df) >= 26:
        macd_obj = MACD(close=df['close'], window_slow=26, window_fast=12, window_sign=9)
        macd_val = macd_obj.macd().iloc[-1]
        macd_sig_line = macd_obj.macd_signal().iloc[-1]
        if not np.isnan(macd_val) and not np.isnan(macd_sig_line):
           sig = "Buy" if macd_val > macd_sig_line else "Sell"
           result["items"].append({"name": "MACD (12,26)", "value": macd_val, "signal": sig})
           result["summary"][sig.lower()] += 1

    # 3. STOCH (14, 3, 3)
    stoch_obj = StochasticOscillator(high=df['high'], low=df['low'], close=df['close'], window=14, smooth_window=3)
    k_val = stoch_obj.stoch().iloc[-1]
    # To get D line (not strictly needed for just K signal, but good standard)
    # d_val = stoch_obj.stoch_signal().iloc[-1]
    if not np.isnan(k_val):
        sig = "Buy" if k_val < 20 else ("Sell" if k_val > 80 else "Neutral")
        result["items"].append({"name": "STOCH (14,3,3)", "value": k_val, "signal": sig})
        result["summary"][sig.lower()] += 1
            
    # 4. CCI (14)
    cci_val = CCIIndicator(high=df['high'], low=df['low'], close=df['close'], window=14).cci().iloc[-1]
    if not np.isnan(cci_val):
        sig = "Buy" if cci_val < -100 else ("Sell" if cci_val > 100 else "Neutral")
        result["items"].append({"name": "CCI (14)", "value": cci_val, "signal": sig})
        result["summary"][sig.lower()] += 1
        
    # 5. ATR (14) - No specific buy/sell, just value
    atr_val = AverageTrueRange(high=df['high'], low=df['low'], close=df['close'], window=14).average_true_range().iloc[-1]
    if not np.isnan(atr_val):
        result["items"].append({"name": "ATR (14)", "value": atr_val, "signal": "Neutral"})
        result["summary"]["neutral"] += 1

    return result

def _calculate_pivot_points(df: pd.DataFrame) -> Dict[str, float]:
    """Calculates Classic Pivot Points from the most recent completed period."""
    if len(df) < 2:
        return {}
    
    # Use previous day for pivot calculation
    prev_row = df.iloc[-2]
    high, low, close = prev_row['high'], prev_row['low'], prev_row['close']
    
    pivot = (high + low + close) / 3
    r1 = (2 * pivot) - low
    s1 = (2 * pivot) - high
    r2 = pivot + (high - low)
    s2 = pivot - (high - low)
    r3 = high + 2 * (pivot - low)
    s3 = low - 2 * (high - pivot)

    return {
        "S3": s3, "S2": s2, "S1": s1,
        "Pivot": pivot,
        "R1": r1, "R2": r2, "R3": r3
    }
    
def get_technical_analysis(db: Session, stock_id: int) -> Optional[Dict[str, Any]]:
    # Fetch max 250 rows for 200-day MA calculation
    klines = db.query(DailyKline).filter(DailyKline.stock_id == stock_id).order_by(desc(DailyKline.date)).limit(250).all()
    if not klines or len(klines) < 20: 
        return None  # Insufficient data
        
    klines.reverse()  # chronological order
    
    df = pd.DataFrame([{
        "date": k.date, "open": k.open, "high": k.high, 
        "low": k.low, "close": k.close, "volume": k.volume
    } for k in klines])
    
    ma_data = _calculate_moving_averages(df)
    osc_data = _calculate_oscillators(df)
    pivots = _calculate_pivot_points(df)
    
    # Calculate overall consensus
    total_buy = ma_data["summary"]["buy"] + osc_data["summary"]["buy"]
    total_sell = ma_data["summary"]["sell"] + osc_data["summary"]["sell"]
    total_neutral = ma_data["summary"]["neutral"] + osc_data["summary"]["neutral"]
    
    if total_buy > total_sell * 1.5:
        overall_signal = "Strong Buy"
    elif total_buy > total_sell:
        overall_signal = "Buy"
    elif total_sell > total_buy * 1.5:
        overall_signal = "Strong Sell"
    elif total_sell > total_buy:
        overall_signal = "Sell"
    else:
        overall_signal = "Neutral"

    return {
        "summary": {
            "signal": overall_signal,
            "buy": total_buy,
            "sell": total_sell,
            "neutral": total_neutral
        },
        "moving_averages": ma_data,
        "oscillators": osc_data,
        "pivot_points": pivots,
        "last_price": df['close'].iloc[-1]
    }
    
def get_analyst_consensus(db: Session, stock_id: int) -> Optional[Dict[str, Any]]:
    forecast = db.query(AnalystForecast).filter_by(stock_id=stock_id).first()
    if not forecast:
        return None
        
    return {
        "consensus": forecast.consensus,
        "target_high": float(forecast.target_high) if forecast.target_high else None,
        "target_low": float(forecast.target_low) if forecast.target_low else None,
        "target_median": float(forecast.target_median) if forecast.target_median else None,
        "target_average": float(forecast.target_average) if forecast.target_average else None
    }
