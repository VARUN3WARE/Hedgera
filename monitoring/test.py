"""
Live OHLCV (Open, High, Low, Close, Volume) Data Monitor
Continuously displays real-time OHLCV data from Alpaca
"""

import alpaca_trade_api as tradeapi
import pandas as pd
import time
from datetime import datetime, timezone, timedelta
import os

# Alpaca API credentials
API_KEY = "PKR3LHNFV6N6NTYMKM3JNYV5CM"
API_SECRET = "4W7bpaszUscRipKxqUGhzMm3uJJJeFUNcUXxsySHiSgi"
API_BASE_URL = 'https://paper-api.alpaca.markets'

# Stocks to monitor
# TICKERS = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 'META', 'NVDA', 'JPM', 'V', 'WMT',
#            'JNJ', 'PG', 'MA', 'UNH', 'HD', 'DIS', 'BAC', 'XOM', 'KO', 'PFE']
# SYMBOLS=AXP,AMGN,AAPL,BA,CAT,CSCO,CVX,GS,HD,HON,IBM,INTC,JNJ,KO,JPM,MCD,MMM,MRK,MSFT,NKE,PG,TRV,UNH,V,VZ,WBA,WMT,DIS,DOW,CRM,VIXY
TICKERS = ['AXP', 'AMGN', 'AAPL', 'BA', 'CAT', 'CSCO', 'CVX', 'GS', 'HD', 'HON', 'IBM', 
           'INTC', 'JNJ', 'KO', 'JPM', 'MCD', 'MMM', 'MRK', 'MSFT', 'NKE', 'PG', 'TRV', 
           'UNH', 'V', 'VZ', 'WBA', 'WMT', 'DIS', 'DOW', 'CRM']

TIMEFRAME = '1Min'  # 1 minute bars (can change to 5Min, 15Min, 1H, 1D, etc)
REFRESH_RATE = 60   # seconds between updates


class LiveOHLCVMonitor:
    """Live OHLCV data display"""
    
    def __init__(self, api_key, api_secret, api_base_url, tickers, timeframe='1Min', refresh_rate=60):
        self.tickers = tickers
        self.timeframe = timeframe
        self.refresh_rate = refresh_rate
        self.iteration = 0
        self.ohlcv_history = {}
        
        # Connect to Alpaca
        try:
            self.alpaca = tradeapi.REST(api_key, api_secret, api_base_url, 'v2')
            print("✅ Connected to Alpaca API\n")
        except Exception as e:
            raise ValueError(f'Failed to connect to Alpaca: {e}')
    
    def clear_screen(self):
        """Clear console"""
        os.system('clear' if os.name == 'posix' else 'cls')
    
    def get_latest_ohlcv(self):
        """Get latest OHLCV bars for all tickers"""
        try:
            # Get Alpaca server timestamp first
            try:
                clock = self.alpaca.get_clock()
                alpaca_timestamp = clock.timestamp
            except:
                alpaca_timestamp = datetime.now(timezone.utc)
            
            ohlcv_data = {}
            
            for ticker in self.tickers:
                try:
                    # Get latest bar
                    bar = self.alpaca.get_latest_bar(ticker)
                    
                    if bar:
                        ohlcv_data[ticker] = {
                            'open': bar.o,
                            'high': bar.h,
                            'low': bar.l,
                            'close': bar.c,
                            'volume': bar.v,
                            'timestamp': bar.t,
                            'vwap': getattr(bar, 'vwap', None),
                            'alpaca_server_time': alpaca_timestamp,
                            'fetch_time_utc': datetime.now(timezone.utc)
                        }
                    else:
                        ohlcv_data[ticker] = None
                        
                except Exception as e:
                    print(f"⚠️  Could not get OHLCV for {ticker}: {e}")
                    ohlcv_data[ticker] = None
            
            return ohlcv_data
            
        except Exception as e:
            print(f"⚠️  Error fetching OHLCV data: {e}")
            return {}
    
    def check_market_status(self):
        """Check if market is open"""
        try:
            clock = self.alpaca.get_clock()
            return clock.is_open
        except:
            return None
    
    def calculate_change(self, ticker, current_close):
        """Calculate change from previous close"""
        if ticker in self.ohlcv_history:
            prev_close = self.ohlcv_history[ticker]['close']
            change = current_close - prev_close
            change_pct = (change / prev_close) * 100
            return change, change_pct
        return 0, 0
    
    def calculate_bar_change(self, ohlcv):
        """Calculate change within the bar (Open to Close)"""
        if ohlcv['open'] > 0:
            change = ohlcv['close'] - ohlcv['open']
            change_pct = (change / ohlcv['open']) * 100
            return change, change_pct
        return 0, 0
    
    def format_volume(self, volume):
        """Format volume with K, M suffix"""
        if volume >= 1e6:
            return f"{volume/1e6:.1f}M"
        elif volume >= 1e3:
            return f"{volume/1e3:.1f}K"
        else:
            return f"{int(volume)}"
    
    def display_ohlcv(self, ohlcv_data, is_market_open):
        """Display OHLCV data in a formatted table"""
        self.clear_screen()
        
        print("=" * 140)
        print(f"📊 LIVE OHLCV DATA MONITOR - Update #{self.iteration}")
        print(f"🕐 Local: {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')} | Timeframe: {self.timeframe}")
        
        # Show Alpaca server time if available
        if ohlcv_data:
            first_ticker_data = next((data for data in ohlcv_data.values() if data is not None), None)
            if first_ticker_data and 'alpaca_server_time' in first_ticker_data:
                alpaca_time = first_ticker_data['alpaca_server_time']
                if isinstance(alpaca_time, datetime):
                    print(f"📡 Alpaca Server: {alpaca_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
        
        # Market status
        if is_market_open is not None:
            status = "🟢 MARKET OPEN" if is_market_open else "🔴 MARKET CLOSED"
            print(f"📈 {status}")
        
        print("=" * 140)
        print(f"\n{'#':<4} {'Ticker':<8} {'Open':>10} {'High':>10} {'Low':>10} {'Close':>10} {'Change':>10} "
              f"{'Change%':>10} {'Volume':>12} {'Bar Time':>20} {'Fetch Time':>20}")
        print("-" * 140)
        
        # Display OHLCV data
        valid_ohlcv = [(ticker, data) for ticker, data in ohlcv_data.items() if data is not None]
        valid_ohlcv.sort(key=lambda x: x[1]['close'], reverse=True)
        
        for idx, (ticker, ohlcv) in enumerate(valid_ohlcv, 1):
            # Calculate changes
            change, change_pct = self.calculate_bar_change(ohlcv)
            
            # Format change with emoji
            if change > 0:
                change_emoji = "📈"
                change_str = f"+${change:.2f}"
                pct_str = f"+{change_pct:.2f}%"
            elif change < 0:
                change_emoji = "📉"
                change_str = f"-${abs(change):.2f}"
                pct_str = f"{change_pct:.2f}%"
            else:
                change_emoji = "➖"
                change_str = "$0.00"
                pct_str = "0.00%"
            
            # Convert bar timestamp to UTC
            if ohlcv['timestamp'].tzinfo is None:
                bar_timestamp_utc = ohlcv['timestamp'].replace(tzinfo=timezone.utc)
            else:
                bar_timestamp_utc = ohlcv['timestamp'].astimezone(timezone.utc)
            bar_time = bar_timestamp_utc.strftime('%H:%M:%S UTC')
            
            # Format fetch timestamp
            fetch_time_utc = ohlcv.get('fetch_time_utc', datetime.now(timezone.utc))
            if isinstance(fetch_time_utc, datetime):
                fetch_time = fetch_time_utc.strftime('%H:%M:%S UTC')
            else:
                fetch_time = 'N/A'
            
            # Format volume
            volume_str = self.format_volume(ohlcv['volume'])
            
            print(f"{idx:<4} {ticker:<8} ${ohlcv['open']:>9.2f} ${ohlcv['high']:>9.2f} ${ohlcv['low']:>9.2f} "
                  f"${ohlcv['close']:>9.2f} {change_emoji}{change_str:>9} {pct_str:>9} {volume_str:>12} {bar_time:>20} {fetch_time:>20}")
            
            # Update history
            self.ohlcv_history[ticker] = ohlcv
        
        print("-" * 140)
        print(f"\nMonitoring: {len(valid_ohlcv)} stocks | Timeframe: {self.timeframe} | Refresh: {self.refresh_rate}s")
        print("=" * 140)
        print("⏳ Waiting for next update... (Press Ctrl+C to stop)")
    
    def display_summary_stats(self, ohlcv_data):
        """Display summary statistics"""
        valid_ohlcv = [(ticker, data) for ticker, data in ohlcv_data.items() if data is not None]
        
        if not valid_ohlcv:
            return
        
        closes = [data['close'] for _, data in valid_ohlcv]
        volumes = [data['volume'] for _, data in valid_ohlcv]
        
        print("\n📈 MARKET SUMMARY")
        print("-" * 140)
        print(f"   Average Price: ${sum(closes)/len(closes):.2f}")
        print(f"   Highest Price: ${max(closes):.2f}")
        print(f"   Lowest Price: ${min(closes):.2f}")
        print(f"   Total Volume: {self.format_volume(sum(volumes))}")
        print(f"   Average Volume: {self.format_volume(sum(volumes)/len(volumes))}")
        print("-" * 140)
    
    def run(self):
        """Main loop"""
        print("🚀 Starting Live OHLCV Monitor...")
        print(f"📡 Monitoring {len(self.tickers)} stocks")
        print(f"⏱️  Timeframe: {self.timeframe}")
        print(f"⏳ Refresh rate: {self.refresh_rate} seconds\n")
        time.sleep(2)
        
        try:
            while True:
                self.iteration += 1
                
                # Get market status
                is_market_open = self.check_market_status()
                
                # Get latest OHLCV
                ohlcv_data = self.get_latest_ohlcv()
                
                # Display
                if ohlcv_data:
                    self.display_ohlcv(ohlcv_data, is_market_open)
                    self.display_summary_stats(ohlcv_data)
                else:
                    print("⚠️  No OHLCV data available")
                
                # Wait
                time.sleep(self.refresh_rate)
                
        except KeyboardInterrupt:
            print("\n\n🛑 OHLCV Monitor stopped")
            print(f"   Total updates: {self.iteration}")
            print("✅ Exiting...")
        except Exception as e:
            print(f"\n❌ Error: {e}")
            raise


def main():
    """Main function"""
    print("=" * 140)
    print("📊 LIVE OHLCV DATA MONITOR")
    print("=" * 140)
    print(f"\nMonitoring {len(TICKERS)} stocks:")
    print(f"{', '.join(TICKERS)}\n")
    print(f"Timeframe: {TIMEFRAME}")
    print(f"Refresh rate: {REFRESH_RATE} seconds")
    print("=" * 140)
    print()
    
    # Create monitor
    monitor = LiveOHLCVMonitor(
        api_key=API_KEY,
        api_secret=API_SECRET,
        api_base_url=API_BASE_URL,
        tickers=TICKERS,
        timeframe=TIMEFRAME,
        refresh_rate=REFRESH_RATE
    )
    
    # Start monitoring
    monitor.run()


if __name__ == "__main__":
    main()