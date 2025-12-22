-- Staging model for stock metadata
-- Provides company and ticker reference data

WITH source AS (
    SELECT * FROM {{ source('stock_market', 'stock_metadata') }}
)

SELECT
    ticker,
    company_name,
    sector,
    industry,
    market_cap,
    country,
    exchange,
    currency,
    is_active,
    created_at,
    updated_at

FROM source
WHERE is_active = TRUE
