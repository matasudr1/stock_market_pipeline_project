-- Stock Market Data Pipeline - Database Initialization
-- Schema for raw stock data and transformed analytics

-- Create database extensions
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Raw stock price data from yfinance
CREATE TABLE IF NOT EXISTS raw_stock_prices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    ticker VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open DECIMAL(12, 4),
    high DECIMAL(12, 4),
    low DECIMAL(12, 4),
    close DECIMAL(12, 4),
    adj_close DECIMAL(12, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(ticker, date)
);

-- Stock metadata
CREATE TABLE IF NOT EXISTS stock_metadata (
    ticker VARCHAR(10) PRIMARY KEY,
    company_name VARCHAR(255),
    sector VARCHAR(100),
    industry VARCHAR(100),
    market_cap BIGINT,
    country VARCHAR(50),
    exchange VARCHAR(50),
    currency VARCHAR(10),
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Market indices for benchmarking
CREATE TABLE IF NOT EXISTS market_indices (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    index_name VARCHAR(50) NOT NULL,
    date DATE NOT NULL,
    close_price DECIMAL(12, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(index_name, date)
);

-- Pipeline execution logs
CREATE TABLE IF NOT EXISTS pipeline_logs (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    pipeline_name VARCHAR(100) NOT NULL,
    execution_date DATE NOT NULL,
    status VARCHAR(20) NOT NULL, -- SUCCESS, FAILED, RUNNING
    records_processed INT,
    error_message TEXT,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for query performance
CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date ON raw_stock_prices(ticker, date DESC);
CREATE INDEX IF NOT EXISTS idx_stock_prices_date ON raw_stock_prices(date DESC);
CREATE INDEX IF NOT EXISTS idx_market_indices_date ON market_indices(index_name, date DESC);
CREATE INDEX IF NOT EXISTS idx_pipeline_logs_date ON pipeline_logs(execution_date DESC);

-- Insert popular stock tickers to track
INSERT INTO stock_metadata (ticker, company_name, sector, exchange) VALUES
    ('AAPL', 'Apple Inc.', 'Technology', 'NASDAQ'),
    ('MSFT', 'Microsoft Corporation', 'Technology', 'NASDAQ'),
    ('GOOGL', 'Alphabet Inc.', 'Technology', 'NASDAQ'),
    ('AMZN', 'Amazon.com Inc.', 'Consumer Cyclical', 'NASDAQ'),
    ('TSLA', 'Tesla Inc.', 'Automotive', 'NASDAQ'),
    ('NVDA', 'NVIDIA Corporation', 'Technology', 'NASDAQ'),
    ('META', 'Meta Platforms Inc.', 'Technology', 'NASDAQ'),
    ('JPM', 'JPMorgan Chase & Co.', 'Financial Services', 'NYSE'),
    ('V', 'Visa Inc.', 'Financial Services', 'NYSE'),
    ('WMT', 'Walmart Inc.', 'Consumer Defensive', 'NYSE')
ON CONFLICT (ticker) DO NOTHING;

-- Insert market indices
INSERT INTO stock_metadata (ticker, company_name, sector, exchange) VALUES
    ('^GSPC', 'S&P 500', 'Index', 'INDEX'),
    ('^DJI', 'Dow Jones Industrial Average', 'Index', 'INDEX'),
    ('^IXIC', 'NASDAQ Composite', 'Index', 'INDEX'),
    ('^VIX', 'CBOE Volatility Index', 'Index', 'INDEX')
ON CONFLICT (ticker) DO NOTHING;

-- Views for convenience
CREATE OR REPLACE VIEW vw_latest_prices AS
SELECT 
    sp.ticker,
    sm.company_name,
    sp.date,
    sp.close,
    sp.volume,
    sp.adj_close
FROM raw_stock_prices sp
JOIN stock_metadata sm ON sp.ticker = sm.ticker
WHERE sp.date = (
    SELECT MAX(date) 
    FROM raw_stock_prices 
    WHERE ticker = sp.ticker
)
ORDER BY sp.ticker;

COMMENT ON TABLE raw_stock_prices IS 'Raw OHLCV data from yfinance API';
COMMENT ON TABLE stock_metadata IS 'Company information and ticker metadata';
COMMENT ON TABLE market_indices IS 'Major market indices for benchmarking';
COMMENT ON TABLE pipeline_logs IS 'ETL pipeline execution history';
