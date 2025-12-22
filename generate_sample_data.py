import psycopg2
from datetime import datetime, timedelta
import random

# Connect to database
conn = psycopg2.connect("postgresql://postgres:postgres@postgres:5432/stock_market")
cur = conn.cursor()

# Sample tickers with realistic price ranges
tickers_data = {
    'AAPL': {'base': 180, 'volatility': 0.02},
    'MSFT': {'base': 370, 'volatility': 0.015},
    'GOOGL': {'base': 140, 'volatility': 0.025},
    'AMZN': {'base': 150, 'volatility': 0.02},
    'TSLA': {'base': 250, 'volatility': 0.04},
    'NVDA': {'base': 490, 'volatility': 0.03},
    'META': {'base': 350, 'volatility': 0.025},
    'JPM': {'base': 160, 'volatility': 0.015},
    'V': {'base': 260, 'volatility': 0.01},
    'WMT': {'base': 165, 'volatility': 0.012},
}

# Generate 90 days of data
end_date = datetime.now().date()
start_date = end_date - timedelta(days=90)

print(f"Generating sample data from {start_date} to {end_date}")

records_inserted = 0
current_date = start_date

while current_date <= end_date:
    # Skip weekends
    if current_date.weekday() >= 5:
        current_date += timedelta(days=1)
        continue
    
    for ticker, params in tickers_data.items():
        base_price = params['base']
        volatility = params['volatility']
        
        # Generate realistic OHLCV data
        open_price = base_price * (1 + random.uniform(-volatility, volatility))
        close_price = open_price * (1 + random.uniform(-volatility, volatility))
        high_price = max(open_price, close_price) * (1 + random.uniform(0, volatility/2))
        low_price = min(open_price, close_price) * (1 - random.uniform(0, volatility/2))
        volume = random.randint(50000000, 150000000)
        
        try:
            cur.execute("""
                INSERT INTO raw_stock_prices 
                (ticker, date, open, high, low, close, adj_close, volume)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) DO NOTHING
            """, (ticker, current_date, round(open_price, 2), round(high_price, 2), 
                  round(low_price, 2), round(close_price, 2), round(close_price, 2), volume))
            records_inserted += 1
        except Exception as e:
            print(f"Error inserting {ticker} on {current_date}: {e}")
    
    current_date += timedelta(days=1)

conn.commit()
print(f"\\nInserted {records_inserted} records")

# Verify
cur.execute("SELECT ticker, COUNT(*) FROM raw_stock_prices GROUP BY ticker ORDER BY ticker")
results = cur.fetchall()
print("\\nRecords per ticker:")
for ticker, count in results:
    print(f"  {ticker}: {count}")

cur.close()
conn.close()
