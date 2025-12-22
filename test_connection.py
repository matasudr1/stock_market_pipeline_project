import sys
sys.path.insert(0, '/opt/airflow/ingestion')
from stock_extractor import StockDataExtractor
from datetime import datetime, timedelta

# Test connection and fetch
extractor = StockDataExtractor()
print(f"Connection string: {extractor.db_connection_string}")

# Test data fetch
end_date = datetime.now()
start_date = end_date - timedelta(days=30)

tickers = ['AAPL', 'MSFT']
print(f"Fetching data for {tickers}")

df = extractor.fetch_multiple_stocks(
    tickers,
    start_date.strftime('%Y-%m-%d'),
    end_date.strftime('%Y-%m-%d')
)

print(f"Fetched {len(df)} records")
print(df.head())

# Try to load
extractor.load_to_database(df)
print("Data loaded successfully")

# Verify
with extractor.engine.connect() as conn:
    from sqlalchemy import text
    result = conn.execute(text('SELECT COUNT(*) FROM raw_stock_prices'))
    count = result.scalar()
    print(f"Total records in database: {count}")
