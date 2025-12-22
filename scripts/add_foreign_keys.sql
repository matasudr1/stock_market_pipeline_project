-- Add Foreign Key Constraints to Stock Market Pipeline
-- This enforces referential integrity at the database level

-- 1. Add FK: raw_stock_prices.ticker → stock_metadata.ticker
-- Ensures every stock price has a valid ticker in metadata table
ALTER TABLE raw_stock_prices 
ADD CONSTRAINT fk_stock_prices_ticker 
FOREIGN KEY (ticker) 
REFERENCES stock_metadata(ticker)
ON DELETE CASCADE;  -- If ticker deleted, delete all its prices too

-- 2. Add relationship test in dbt (edit schema.yml)
-- This is the "dbt way" of enforcing relationships

-- To test this, run:
-- docker-compose exec postgres psql -U postgres -d stock_market -f /path/to/add_foreign_keys.sql

-- To verify constraints:
-- \d raw_stock_prices

-- Benefits:
-- ✓ Database enforces data quality
-- ✓ Cannot insert orphan records (ticker not in metadata)
-- ✓ Cascade deletes maintain consistency

-- Drawbacks:
-- ✗ Slower inserts (DB checks FK on every insert)
-- ✗ Need to insert metadata BEFORE prices
-- ✗ Can't handle out-of-order data arrival

-- In production analytics DBs (Snowflake, BigQuery, Redshift):
-- Foreign keys are often NOT enforced for performance
-- Instead, use dbt tests to validate relationships
