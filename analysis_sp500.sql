-- ========================================
-- Data Cleaning & Analysis Script
-- Only includes cleaning steps and analysis queries
-- ========================================

-- ----------------------------------------
-- 1. Data Cleaning
-- ----------------------------------------

-- 1.1 Remove companies with NULL revenue_growth
DELETE FROM finance.companies
 WHERE revenue_growth IS NULL;

-- 1.2 Remove stock_prices rows with NULL critical fields
DELETE FROM finance.stock_prices
 WHERE adj_close IS NULL
    OR close      IS NULL
    OR trade_date IS NULL;

-- 1.3 Drop the unused 'longname' column
ALTER TABLE finance.companies
 DROP COLUMN IF EXISTS longname;

-- 1.4 Remove any stock_prices rows whose symbol is not in companies
DELETE FROM finance.stock_prices
 WHERE symbol NOT IN (SELECT symbol FROM finance.companies);

-- ----------------------------------------
-- 2. Analysis Queries
-- ----------------------------------------

-- 2.1 Sector vs. Overall Market Performance Over Time
--     (average close per sector vs. S&P 500 index)
SELECT
  sa.trade_date,
  sa.sector,
  sa.avg_sector_price,
  idx.sp500_close
FROM (
  SELECT
    sp.trade_date,
    c.sector,
    AVG(sp.close) AS avg_sector_price
  FROM finance.stock_prices AS sp
  JOIN finance.companies    AS c USING(symbol)
  GROUP BY sp.trade_date, c.sector
) AS sa
JOIN finance.index_data AS idx
  ON sa.trade_date = idx.index_date
ORDER BY sa.trade_date, sa.sector;


-- 2.2 Top‐performing Stocks in the Technology Sector
WITH stock_range AS (
  SELECT
    symbol,
    MIN(trade_date) AS start_date,
    MAX(trade_date) AS end_date
  FROM finance.stock_prices
  GROUP BY symbol
),
tech_summary AS (
  SELECT
    sr.symbol,
    c.shortname,
    sr.start_date,
    sr.end_date,
    sp_start.close AS start_price,
    sp_end.close   AS end_price,
    ROUND((sp_end.close - sp_start.close) / sp_start.close * 100, 2) AS pct_change
  FROM stock_range sr
  JOIN finance.companies      c USING(symbol)
  JOIN finance.stock_prices  sp_start 
    ON sr.symbol = sp_start.symbol AND sr.start_date = sp_start.trade_date
  JOIN finance.stock_prices  sp_end 
    ON sr.symbol = sp_end.symbol   AND sr.end_date   = sp_end.trade_date
  WHERE c.sector = 'Technology'
)
SELECT *
FROM tech_summary
ORDER BY pct_change DESC
LIMIT 10;


-- 2.3 Top‐performing Stock in Each Sector
WITH stock_range AS (
  SELECT
    symbol,
    MIN(trade_date) AS start_date,
    MAX(trade_date) AS end_date
  FROM finance.stock_prices
  GROUP BY symbol
),
stock_growth AS (
  SELECT
    c.sector,
    sr.symbol,
    c.shortname,
    sp_start.close AS start_price,
    sp_end.close   AS end_price,
    ROUND((sp_end.close - sp_start.close) / sp_start.close * 100, 2) AS pct_change
  FROM stock_range sr
  JOIN finance.companies     c USING(symbol)
  JOIN finance.stock_prices sp_start 
    ON sr.symbol = sp_start.symbol AND sr.start_date = sp_start.trade_date
  JOIN finance.stock_prices sp_end 
    ON sr.symbol = sp_end.symbol   AND sr.end_date   = sp_end.trade_date
),
ranked AS (
  SELECT
    *,
    RANK() OVER (PARTITION BY sector ORDER BY pct_change DESC) AS rnk
  FROM stock_growth
)
SELECT sector, symbol, shortname, start_price, end_price, pct_change
FROM ranked
WHERE rnk = 1
ORDER BY pct_change DESC;


-- 2.4 7-Day Moving Average for One Company (e.g. MSFT)
SELECT
  symbol,
  trade_date,
  close,
  ROUND(
    AVG(close) OVER (
      PARTITION BY symbol
      ORDER BY trade_date
      ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
    )
  , 2) AS moving_avg_7d
FROM finance.stock_prices
WHERE symbol = 'MSFT'
ORDER BY trade_date;


-- 2.5 Trading Volume Trend Across Time
SELECT
  trade_date,
  SUM(volume) AS total_volume
FROM finance.stock_prices
GROUP BY trade_date
ORDER BY trade_date;


-- 2.6 Sector-wise Total Market Capitalization
SELECT
  sector,
  ROUND(SUM(market_cap)/1e9, 2) AS total_market_cap_billion
FROM finance.companies
GROUP BY sector
ORDER BY total_market_cap_billion DESC;


-- 2.7 High-Volatility Stocks
WITH daily_return AS (
  SELECT
    symbol,
    trade_date,
    (close - LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date))
      / LAG(close) OVER (PARTITION BY symbol ORDER BY trade_date) AS return
  FROM finance.stock_prices
),
volatility AS (
  SELECT
    symbol,
    STDDEV(return) AS volatility
  FROM daily_return
  WHERE return IS NOT NULL
  GROUP BY symbol
)
SELECT *
FROM volatility
WHERE volatility > 0.05
ORDER BY volatility DESC;
