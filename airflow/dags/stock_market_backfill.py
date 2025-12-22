"""
Stock Market Backfill Pipeline
One-time DAG to backfill historical stock data
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import sys
import os

sys.path.insert(0, '/opt/airflow/ingestion')
from stock_extractor import StockDataExtractor

default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'stock_market_backfill',
    default_args=default_args,
    description='Backfill historical stock market data',
    schedule_interval=None,  # Manual trigger only
    start_date=days_ago(1),
    catchup=False,
    tags=['finance', 'stock_market', 'backfill'],
)


def backfill_stock_data(**context):
    """Backfill historical stock data - fetches 100 days from Alpha Vantage"""
    
    extractor = StockDataExtractor()
    
    # Alpha Vantage 'compact' returns last 100 trading days
    # We use today as end_date and 150 days back as start to capture all 100 trading days
    end_date = datetime.now()
    start_date = end_date - timedelta(days=150)  # ~100 trading days
    
    # Get active tickers
    tickers = extractor.get_active_tickers()
    
    if not tickers:
        tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA', 
                   'NVDA', 'META', 'JPM', 'V', 'WMT']
    
    print(f"Backfilling {len(tickers)} tickers from {start_date.date()} to {end_date.date()}")
    print(f"Note: Alpha Vantage returns last 100 trading days per ticker")
    print(f"Rate limit: 5 calls/minute - this will take ~{len(tickers) * 13 // 60} minutes")
    
    # Fetch and load data
    df = extractor.fetch_multiple_stocks(
        tickers,
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    
    extractor.load_to_database(df)
    
    return f"Backfilled {len(df)} records for {len(tickers)} tickers"


backfill_task = PythonOperator(
    task_id='backfill_historical_data',
    python_callable=backfill_stock_data,
    provide_context=True,
    dag=dag,
)
