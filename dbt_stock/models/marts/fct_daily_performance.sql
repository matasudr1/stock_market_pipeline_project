-- Daily Stock Performance Summary
-- Aggregates key metrics and rankings for each trading day

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['date', 'ticker']},
        {'columns': ['date']}
    ]
) }}

WITH tech_indicators AS (
    SELECT * FROM {{ ref('fct_technical_indicators') }}
),

metadata AS (
    SELECT * FROM {{ ref('stg_stock_metadata') }}
),

daily_metrics AS (
    SELECT
        ti.ticker,
        m.company_name,
        m.sector,
        m.exchange,
        ti.date,
        ti.close AS price,
        ti.volume,
        ti.daily_return_pct,
        ti.pct_change_1d,
        ti.pct_change_20d,
        ti.volatility_30d,
        ti.rsi_14,
        ti.sma_20,
        ti.sma_50,
        ti.macd,
        ti.trend_signal,
        ti.rsi_signal,
        ti.volume_ratio,
        
        -- Performance Rankings
        ROW_NUMBER() OVER (
            PARTITION BY ti.date 
            ORDER BY ti.pct_change_1d DESC
        ) AS daily_gain_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY ti.date 
            ORDER BY ti.volume DESC
        ) AS volume_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY ti.date 
            ORDER BY ti.volatility_30d DESC
        ) AS volatility_rank,
        
        -- Calculate performance relative to previous day
        LAG(ti.close, 1) OVER (PARTITION BY ti.ticker ORDER BY ti.date) AS prev_close,
        
        -- YTD calculation
        FIRST_VALUE(ti.close) OVER (
            PARTITION BY ti.ticker, EXTRACT(YEAR FROM ti.date)
            ORDER BY ti.date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) AS ytd_start_price

    FROM tech_indicators ti
    LEFT JOIN metadata m ON ti.ticker = m.ticker
),

final AS (
    SELECT
        ticker,
        company_name,
        sector,
        exchange,
        date,
        price,
        volume,
        daily_return_pct,
        pct_change_1d,
        pct_change_20d,
        volatility_30d,
        rsi_14,
        sma_20,
        sma_50,
        macd,
        trend_signal,
        rsi_signal,
        volume_ratio,
        daily_gain_rank,
        volume_rank,
        volatility_rank,
        
        -- Calculate YTD return
        ROUND(
            ((price - ytd_start_price) / NULLIF(ytd_start_price, 0) * 100)::NUMERIC, 
        2) AS ytd_return_pct,
        
        -- Market momentum indicators
        CASE
            WHEN pct_change_1d > 2 AND volume_ratio > 1.5 THEN 'Strong Buy Signal'
            WHEN pct_change_1d > 1 AND rsi_signal = 'Oversold' THEN 'Buy Signal'
            WHEN pct_change_1d < -2 AND volume_ratio > 1.5 THEN 'Strong Sell Signal'
            WHEN pct_change_1d < -1 AND rsi_signal = 'Overbought' THEN 'Sell Signal'
            ELSE 'Hold'
        END AS trading_signal,
        
        -- Risk classification
        CASE
            WHEN volatility_30d > 5 THEN 'High Risk'
            WHEN volatility_30d > 2 THEN 'Medium Risk'
            ELSE 'Low Risk'
        END AS risk_category

    FROM daily_metrics
)

SELECT * FROM final
ORDER BY date DESC, daily_gain_rank
