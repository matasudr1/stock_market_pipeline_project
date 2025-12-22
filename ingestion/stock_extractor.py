"""
Stock Market Data Extractor
Fetches stock price data from Alpha Vantage API
"""
import os
import logging
import time
from datetime import datetime, timedelta
from typing import List, Optional
import pandas as pd
import requests
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StockDataExtractor:
    """Extract stock market data from Alpha Vantage API"""
    
    def __init__(self, db_connection_string: Optional[str] = None):
        """
        Initialize the stock data extractor
        
        Args:
            db_connection_string: PostgreSQL connection string
        """
        self.db_connection_string = db_connection_string or os.getenv(
            'DATABASE_URL',
            'postgresql://postgres:postgres@localhost:5432/stock_market'
        )
        self.api_key = os.getenv('ALPHA_VANTAGE_API_KEY')
        if not self.api_key:
            raise ValueError('ALPHA_VANTAGE_API_KEY environment variable is required')
        self.base_url = 'https://www.alphavantage.co/query'
        self.engine = create_engine(self.db_connection_string)
        logger.info(f"Stock Data Extractor initialized with Alpha Vantage API")
    
    def fetch_stock_data(self, ticker: str, start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch stock data for a given ticker using Alpha Vantage API
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            start_date: Start date in YYYY-MM-DD format (Alpha Vantage returns full history)
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            DataFrame with stock price data
        """
        try:
            logger.info(f"Fetching data for {ticker} from Alpha Vantage")
            
            # Alpha Vantage API call
            # 'compact' returns last 100 trading days, 'full' returns up to 20 years
            params = {
                'function': 'TIME_SERIES_DAILY',
                'symbol': ticker,
                'apikey': self.api_key,
                'outputsize': 'compact',  # Last 100 trading days - sufficient for daily loads
                'datatype': 'json'
            }
            
            response = requests.get(self.base_url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()
            
            # Debug: log keys received
            logger.info(f"API response keys for {ticker}: {list(data.keys())}")
            
            # Check for API errors
            if 'Error Message' in data:
                logger.error(f"Alpha Vantage error for {ticker}: {data['Error Message']}")
                return pd.DataFrame()
            
            if 'Note' in data:
                logger.warning(f"Alpha Vantage rate limit reached: {data['Note']}")
                return pd.DataFrame()
            
            if 'Information' in data:
                logger.warning(f"Alpha Vantage info: {data['Information']}")
                return pd.DataFrame()
            
            if 'Time Series (Daily)' not in data:
                logger.warning(f"No time series data for {ticker}. Response: {str(data)[:200]}")
                return pd.DataFrame()
            
            # Parse the time series data
            time_series = data['Time Series (Daily)']
            
            # Convert to DataFrame
            records = []
            for date_str, values in time_series.items():
                records.append({
                    'date': date_str,
                    'open': float(values['1. open']),
                    'high': float(values['2. high']),
                    'low': float(values['3. low']),
                    'close': float(values['4. close']),
                    'volume': int(values['5. volume'])
                })
            
            df = pd.DataFrame(records)
            df['date'] = pd.to_datetime(df['date']).dt.date
            df['ticker'] = ticker
            df['adj_close'] = df['close']  # Alpha Vantage doesn't provide adj_close in daily endpoint
            
            # Filter by date range
            df = df[(df['date'] >= pd.to_datetime(start_date).date()) & 
                    (df['date'] <= pd.to_datetime(end_date).date())]
            
            # Reorder columns to match schema
            df = df[['ticker', 'date', 'open', 'high', 'low', 'close', 'adj_close', 'volume']]
            
            logger.info(f"Successfully fetched {len(df)} records for {ticker}")
            return df
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Network error fetching data for {ticker}: {str(e)}")
            return pd.DataFrame()
        except Exception as e:
            logger.error(f"Error fetching data for {ticker}: {str(e)}")
            return pd.DataFrame()
    
    def fetch_multiple_stocks(self, tickers: List[str], start_date: str, end_date: str) -> pd.DataFrame:
        """
        Fetch data for multiple stock tickers (sequential to avoid rate limits)
        
        Args:
            tickers: List of stock ticker symbols
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Combined DataFrame with all stock data
        """
        try:
            logger.info(f"Fetching {len(tickers)} tickers from {start_date} to {end_date}")
            
            all_data = []
            
            for i, ticker in enumerate(tickers):
                df = self.fetch_stock_data(ticker, start_date, end_date)
                if not df.empty:
                    all_data.append(df)
                
                # Alpha Vantage free tier: 5 calls/minute, 500 calls/day
                # Add delay between requests to avoid rate limiting
                if i < len(tickers) - 1:
                    logger.info(f"Waiting 13 seconds to avoid rate limit...")
                    time.sleep(13)  # 5 calls per 60 seconds = 12 seconds per call
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                logger.info(f"Total records fetched: {len(combined_df)}")
                return combined_df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Error fetching multiple stocks: {str(e)}")
            return pd.DataFrame()
        
        if all_data:
            combined_df = pd.concat(all_data, ignore_index=True)
            logger.info(f"Total records fetched: {len(combined_df)}")
            return combined_df
        
        return pd.DataFrame()
    
    def load_to_database(self, df: pd.DataFrame, table_name: str = 'raw_stock_prices'):
        """
        Load DataFrame to PostgreSQL database
        
        Args:
            df: DataFrame to load
            table_name: Target table name
        """
        if df.empty:
            logger.warning("No data to load")
            return
        
        try:
            # Use upsert to handle duplicates
            records_loaded = 0
            
            with self.engine.begin() as conn:  # begin() auto-commits on success
                for _, row in df.iterrows():
                    query = text("""
                        INSERT INTO raw_stock_prices 
                        (ticker, date, open, high, low, close, adj_close, volume)
                        VALUES 
                        (:ticker, :date, :open, :high, :low, :close, :adj_close, :volume)
                        ON CONFLICT (ticker, date) 
                        DO UPDATE SET
                            open = EXCLUDED.open,
                            high = EXCLUDED.high,
                            low = EXCLUDED.low,
                            close = EXCLUDED.close,
                            adj_close = EXCLUDED.adj_close,
                            volume = EXCLUDED.volume
                    """)
                    conn.execute(query, {
                        'ticker': row['ticker'],
                        'date': row['date'],
                        'open': float(row['open']),
                        'high': float(row['high']),
                        'low': float(row['low']),
                        'close': float(row['close']),
                        'adj_close': float(row['adj_close']),
                        'volume': int(row['volume'])
                    })
                    records_loaded += 1
            
            logger.info(f"Successfully loaded {records_loaded} records to {table_name}")
            
        except SQLAlchemyError as e:
            logger.error(f"Database error: {str(e)}")
            raise
    
    def get_active_tickers(self) -> List[str]:
        """Get list of active tickers from database"""
        try:
            query = text("SELECT ticker FROM stock_metadata WHERE is_active = true ORDER BY ticker")
            with self.engine.connect() as conn:
                result = conn.execute(query)
                tickers = [row[0] for row in result]
            logger.info(f"Found {len(tickers)} active tickers")
            return tickers
        except SQLAlchemyError as e:
            logger.error(f"Error fetching tickers: {str(e)}")
            return []
    
    def log_pipeline_execution(self, pipeline_name: str, execution_date: str, status: str, 
                               records_processed: int = 0, error_message: str = None):
        """Log pipeline execution to database"""
        try:
            query = text("""
                INSERT INTO pipeline_logs 
                (pipeline_name, execution_date, status, records_processed, error_message)
                VALUES (:pipeline_name, :execution_date, :status, :records_processed, :error_message)
            """)
            with self.engine.begin() as conn:  # begin() auto-commits on success
                conn.execute(query, {
                    'pipeline_name': pipeline_name,
                    'execution_date': execution_date,
                    'status': status,
                    'records_processed': records_processed,
                    'error_message': error_message
                })
        except SQLAlchemyError as e:
            logger.error(f"Error logging pipeline execution: {str(e)}")


# Main execution for testing
if __name__ == "__main__":
    extractor = StockDataExtractor()
    
    # Test with sample tickers
    tickers = ['AAPL', 'MSFT', 'GOOGL']
    end_date = datetime.now()
    start_date = end_date - timedelta(days=30)
    
    df = extractor.fetch_multiple_stocks(
        tickers,
        start_date.strftime('%Y-%m-%d'),
        end_date.strftime('%Y-%m-%d')
    )
    
    if not df.empty:
        print(f"Fetched {len(df)} records")
        print(df.head(10))
        extractor.load_to_database(df)
    else:
        print("No data fetched")
