-- Stock Portfolio Analytics
-- Provides aggregated views for portfolio analysis and comparison

{{ config(
    materialized='table',
    indexes=[
        {'columns': ['ticker']},
        {'columns': ['analysis_date']}
    ]
) }}

WITH daily_perf AS (
    SELECT * FROM {{ ref('fct_daily_performance') }}
),

-- Get latest trading date
latest_date AS (
    SELECT MAX(date) AS max_date
    FROM daily_perf
),

-- Calculate period returns
stock_returns AS (
    SELECT
        dp.ticker,
        dp.company_name,
        dp.sector,
        ld.max_date AS analysis_date,
        
        -- Latest metrics
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.price END) AS current_price,
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.volume END) AS current_volume,
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.volatility_30d END) AS volatility_30d,
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.rsi_14 END) AS rsi_14,
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.trend_signal END) AS trend_signal,
        
        -- Period returns
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.pct_change_1d END) AS return_1d,
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.pct_change_20d END) AS return_20d,
        MAX(CASE WHEN dp.date = ld.max_date THEN dp.ytd_return_pct END) AS return_ytd,
        
        -- Calculate additional period returns
        ROUND(
            ((MAX(CASE WHEN dp.date = ld.max_date THEN dp.price END) - 
              MAX(CASE WHEN dp.date = ld.max_date - INTERVAL '90 days' THEN dp.price END)) / 
              NULLIF(MAX(CASE WHEN dp.date = ld.max_date - INTERVAL '90 days' THEN dp.price END), 0) * 100)::NUMERIC,
        2) AS return_90d,
        
        -- Average volume
        AVG(dp.volume) AS avg_volume_30d,
        
        -- Sharpe ratio approximation (assuming 2% risk-free rate)
        ROUND(
            ((AVG(dp.daily_return_pct) * 252 - 2) / NULLIF(STDDEV(dp.daily_return_pct) * SQRT(252), 0))::NUMERIC,
        2) AS sharpe_ratio,
        
        -- Maximum drawdown
        ROUND(
            (MIN(dp.pct_change_20d))::NUMERIC,
        2) AS max_drawdown_20d,
        
        -- Win rate (percentage of positive days)
        ROUND(
            (COUNT(CASE WHEN dp.daily_return_pct > 0 THEN 1 END)::NUMERIC / 
             NULLIF(COUNT(*)::NUMERIC, 0) * 100),
        2) AS win_rate_pct

    FROM daily_perf dp
    CROSS JOIN latest_date ld
    WHERE dp.date >= ld.max_date - INTERVAL '90 days'
    GROUP BY dp.ticker, dp.company_name, dp.sector, ld.max_date
),

-- Sector aggregations
sector_performance AS (
    SELECT
        sector,
        analysis_date,
        COUNT(DISTINCT ticker) AS stocks_in_sector,
        ROUND(AVG(return_1d)::NUMERIC, 2) AS sector_return_1d,
        ROUND(AVG(return_20d)::NUMERIC, 2) AS sector_return_20d,
        ROUND(AVG(volatility_30d)::NUMERIC, 2) AS sector_volatility,
        ROUND(AVG(sharpe_ratio)::NUMERIC, 2) AS sector_sharpe_ratio
    FROM stock_returns
    GROUP BY sector, analysis_date
),

final AS (
    SELECT
        sr.*,
        sp.sector_return_1d,
        sp.sector_return_20d,
        sp.sector_volatility,
        sp.sector_sharpe_ratio,
        
        -- Relative performance to sector
        ROUND((sr.return_1d - sp.sector_return_1d)::NUMERIC, 2) AS excess_return_1d,
        ROUND((sr.return_20d - sp.sector_return_20d)::NUMERIC, 2) AS excess_return_20d,
        
        -- Performance grade
        CASE
            WHEN sr.return_20d > 10 AND sr.sharpe_ratio > 1 THEN 'A'
            WHEN sr.return_20d > 5 AND sr.sharpe_ratio > 0.5 THEN 'B'
            WHEN sr.return_20d > 0 THEN 'C'
            WHEN sr.return_20d > -5 THEN 'D'
            ELSE 'F'
        END AS performance_grade,
        
        -- Risk category based on volatility
        CASE
            WHEN sr.volatility_30d < 15 THEN 'Low Risk'
            WHEN sr.volatility_30d BETWEEN 15 AND 30 THEN 'Medium Risk'
            ELSE 'High Risk'
        END AS risk_category,
        
        -- Investment recommendation
        CASE
            WHEN sr.trend_signal = 'Bullish' 
                AND sr.rsi_14 BETWEEN 40 AND 60 
                AND sr.sharpe_ratio > 0.5 
            THEN 'Strong Buy'
            WHEN sr.trend_signal = 'Bullish' 
                AND sr.return_20d > 0 
            THEN 'Buy'
            WHEN sr.trend_signal = 'Bearish' 
                AND sr.rsi_14 > 70 
            THEN 'Sell'
            WHEN sr.trend_signal = 'Bearish' 
                AND sr.return_20d < -5 
            THEN 'Strong Sell'
            ELSE 'Hold'
        END AS recommendation

    FROM stock_returns sr
    LEFT JOIN sector_performance sp 
        ON sr.sector = sp.sector 
        AND sr.analysis_date = sp.analysis_date
)

SELECT * FROM final
ORDER BY return_20d DESC
