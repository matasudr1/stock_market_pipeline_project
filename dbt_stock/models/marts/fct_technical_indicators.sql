-- Technical Indicators and Financial Metrics
-- Calculates moving averages, volatility, RSI, and other key indicators

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ticker', 'date'], 'unique': True},
        {'columns': ['date']}
    ]
) }}

WITH stock_prices AS (
    SELECT * FROM {{ ref('stg_stock_prices') }}
),

-- Calculate moving averages
moving_averages AS (
    SELECT
        ticker,
        date,
        close,
        adj_close,
        volume,
        daily_return_pct,
        
        -- Simple Moving Averages (SMA)
        AVG(close) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN {{ var('short_ma_window') - 1 }} PRECEDING AND CURRENT ROW
        ) AS sma_20,
        
        AVG(close) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN {{ var('long_ma_window') - 1 }} PRECEDING AND CURRENT ROW
        ) AS sma_50,
        
        AVG(close) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 199 PRECEDING AND CURRENT ROW
        ) AS sma_200,
        
        -- Exponential Moving Average (approximation using simple average for demo)
        AVG(close) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 11 PRECEDING AND CURRENT ROW
        ) AS ema_12,
        
        AVG(close) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 25 PRECEDING AND CURRENT ROW
        ) AS ema_26,
        
        -- Volume moving average
        AVG(volume) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        ) AS volume_ma_20
        
    FROM stock_prices
),

-- Calculate volatility and price ranges
volatility_metrics AS (
    SELECT
        ticker,
        date,
        close,
        adj_close,
        volume,
        daily_return_pct,
        sma_20,
        sma_50,
        sma_200,
        ema_12,
        ema_26,
        volume_ma_20,
        
        -- Historical Volatility (Standard Deviation of Returns)
        STDDEV(daily_return_pct) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN {{ var('volatility_window') - 1 }} PRECEDING AND CURRENT ROW
        ) AS volatility_30d,
        
        -- Bollinger Bands (SMA ± 2 * Standard Deviation)
        sma_20 + (2 * STDDEV(close) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) AS bollinger_upper,
        
        sma_20 - (2 * STDDEV(close) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN 19 PRECEDING AND CURRENT ROW
        )) AS bollinger_lower,
        
        -- Price momentum
        close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date) AS price_change_1d,
        close - LAG(close, 5) OVER (PARTITION BY ticker ORDER BY date) AS price_change_5d,
        close - LAG(close, 20) OVER (PARTITION BY ticker ORDER BY date) AS price_change_20d,
        
        -- Percentage changes
        ROUND(((close - LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date)) / 
            NULLIF(LAG(close, 1) OVER (PARTITION BY ticker ORDER BY date), 0) * 100)::NUMERIC, 2) AS pct_change_1d,
        
        ROUND(((close - LAG(close, 20) OVER (PARTITION BY ticker ORDER BY date)) / 
            NULLIF(LAG(close, 20) OVER (PARTITION BY ticker ORDER BY date), 0) * 100)::NUMERIC, 2) AS pct_change_20d

    FROM moving_averages
),

-- RSI Calculation (Relative Strength Index)
price_changes AS (
    SELECT
        *,
        CASE 
            WHEN price_change_1d > 0 THEN price_change_1d 
            ELSE 0 
        END AS gain,
        CASE 
            WHEN price_change_1d < 0 THEN ABS(price_change_1d) 
            ELSE 0 
        END AS loss
    FROM volatility_metrics
),

rsi_calc AS (
    SELECT
        *,
        AVG(gain) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN {{ var('rsi_window') - 1 }} PRECEDING AND CURRENT ROW
        ) AS avg_gain,
        
        AVG(loss) OVER (
            PARTITION BY ticker 
            ORDER BY date 
            ROWS BETWEEN {{ var('rsi_window') - 1 }} PRECEDING AND CURRENT ROW
        ) AS avg_loss
    FROM price_changes
),

final AS (
    SELECT
        ticker,
        date,
        close,
        adj_close,
        volume,
        daily_return_pct,
        
        -- Moving Averages
        ROUND(sma_20::NUMERIC, 2) AS sma_20,
        ROUND(sma_50::NUMERIC, 2) AS sma_50,
        ROUND(sma_200::NUMERIC, 2) AS sma_200,
        ROUND(ema_12::NUMERIC, 2) AS ema_12,
        ROUND(ema_26::NUMERIC, 2) AS ema_26,
        
        -- MACD (Moving Average Convergence Divergence)
        ROUND((ema_12 - ema_26)::NUMERIC, 2) AS macd,
        
        -- Volatility
        ROUND(volatility_30d::NUMERIC, 2) AS volatility_30d,
        ROUND(bollinger_upper::NUMERIC, 2) AS bollinger_upper,
        ROUND(bollinger_lower::NUMERIC, 2) AS bollinger_lower,
        
        -- RSI
        ROUND(
            CASE 
                WHEN avg_loss = 0 THEN 100
                ELSE 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0))))
            END::NUMERIC, 
        2) AS rsi_14,
        
        -- Price Changes
        ROUND(price_change_1d::NUMERIC, 2) AS price_change_1d,
        ROUND(price_change_5d::NUMERIC, 2) AS price_change_5d,
        ROUND(price_change_20d::NUMERIC, 2) AS price_change_20d,
        pct_change_1d,
        pct_change_20d,
        
        -- Volume Analysis
        ROUND(volume_ma_20::NUMERIC, 0) AS volume_ma_20,
        ROUND((volume / NULLIF(volume_ma_20, 0))::NUMERIC, 2) AS volume_ratio,
        
        -- Trading Signals
        CASE 
            WHEN sma_20 > sma_50 THEN 'Bullish'
            WHEN sma_20 < sma_50 THEN 'Bearish'
            ELSE 'Neutral'
        END AS trend_signal,
        
        CASE 
            WHEN close > bollinger_upper THEN 'Overbought'
            WHEN close < bollinger_lower THEN 'Oversold'
            ELSE 'Normal'
        END AS bollinger_signal,
        
        CASE
            WHEN avg_gain IS NOT NULL AND avg_loss IS NOT NULL THEN
                CASE 
                    WHEN 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))) > 70 THEN 'Overbought'
                    WHEN 100 - (100 / (1 + (avg_gain / NULLIF(avg_loss, 0)))) < 30 THEN 'Oversold'
                    ELSE 'Neutral'
                END
            ELSE 'Neutral'
        END AS rsi_signal

    FROM rsi_calc
)

SELECT * FROM final
WHERE date >= CURRENT_DATE - INTERVAL '2 years'
