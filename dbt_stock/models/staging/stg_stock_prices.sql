-- Staging model for raw stock prices
-- Cleans and prepares raw data for analytics

WITH source AS (
    SELECT * FROM {{ source('stock_market', 'raw_stock_prices') }}
),

cleaned AS (
    SELECT
        ticker,
        date,
        open,
        high,
        low,
        close,
        adj_close,
        volume,
        created_at,
        
        -- Data quality checks
        CASE 
            WHEN high < low THEN TRUE 
            WHEN close > high OR close < low THEN TRUE
            WHEN open > high OR open < low THEN TRUE
            ELSE FALSE 
        END AS has_data_quality_issue,
        
        -- Calculate daily metrics
        ROUND(((close - open) / NULLIF(open, 0) * 100)::NUMERIC, 2) AS daily_return_pct,
        ROUND(((high - low) / NULLIF(low, 0) * 100)::NUMERIC, 2) AS intraday_range_pct,
        close - open AS price_change,
        
        -- Volume metrics
        CASE 
            WHEN volume = 0 THEN TRUE 
            ELSE FALSE 
        END AS is_low_volume

    FROM source
    WHERE 
        date IS NOT NULL
        AND ticker IS NOT NULL
        AND close IS NOT NULL
)

SELECT * FROM cleaned
WHERE NOT has_data_quality_issue
