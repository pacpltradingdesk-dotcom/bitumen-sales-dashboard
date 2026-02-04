import datetime
import random
import yfinance as yf

def get_live_market_data():
    """
    Fetches LIVE market data using yfinance (Yahoo Finance API).
    Includes 7-Day Change calculation.
    """
    data = {}
    tickers = {
        "brent": "BZ=F",
        "wti": "CL=F",
        "usdinr": "INR=X",
        "dxy": "DX-Y.NYB"
    }

    try:
        # Batch fetch for speed
        tickers_list = list(tickers.values())
        df = yf.download(tickers_list, period="7d", interval="1d", progress=False)['Close']
        
        for key, ticker in tickers.items():
            try:
                # Get series for this ticker
                series = df[ticker].dropna()
                
                if len(series) < 2:
                    current_price = 0.0
                    price_7d_ago = 0.0 # Default
                    chg_7d = 0.0
                    
                else:
                    current_price = series.iloc[-1]
                    price_7d_ago = series.iloc[0]
                    # Calculate 7-day % diff
                    chg_7d = ((current_price - price_7d_ago) / price_7d_ago) * 100
                
                # Format
                if key == "usdinr":
                    fmt_val = f"₹{current_price:.2f}"
                    fmt_val_7d = f"₹{price_7d_ago:.2f}"
                elif key == "dxy":
                    fmt_val = f"{current_price:.2f}"
                    fmt_val_7d = f"{price_7d_ago:.2f}"
                else:
                    fmt_val = f"${current_price:.2f}"
                    fmt_val_7d = f"${price_7d_ago:.2f}"
                
                # Determine color (Red = Down, Green = Up) - Standard Finance colors
                color = "green" if chg_7d >= 0 else "red"
                
                data[key] = {
                    "value": fmt_val,
                    "value_7d": fmt_val_7d,
                    "change": f"{chg_7d:+.2f}%",
                    "color": color
                }
            except Exception as e:
                # Individual ticker failure
                data[key] = {"value": "N/A", "value_7d": "N/A", "change": "0.00%", "color": "grey"}

        data['timestamp'] = datetime.datetime.now().strftime("%H:%M IST")
        return data

    except Exception as e:
        print(f"API Error: {e}. Falling back to simulation.")
        return get_simulated_data()

def get_simulated_data():
    """Fallback if internet/API is down."""
    brent = 78.50 + random.uniform(-0.5, 0.5)
    wti = 73.20 + random.uniform(-0.5, 0.5)
    usdinr = 83.15 + random.uniform(-0.1, 0.1)
    dxy = 103.4
    
    # Simulate past prices
    brent_7d = brent / 1.012
    wti_7d = wti / 0.995
    
    return {
        "brent": {"value": f"${brent:.2f}", "value_7d": f"${brent_7d:.2f}", "change": "+1.2%", "color": "green"},
        "wti": {"value": f"${wti:.2f}", "value_7d": f"${wti_7d:.2f}", "change": "-0.5%", "color": "red"},
        "usdinr": {"value": f"₹{usdinr:.2f}", "value_7d": f"₹{83.05:.2f}", "change": "+0.1%", "color": "green"},
        "dxy": {"value": f"{dxy:.2f}", "value_7d": "103.40", "change": "0.0%", "color": "grey"},
        "timestamp": datetime.datetime.now().strftime("%H:%M IST (Offline)")
    }
