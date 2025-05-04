-- Setup script for S&P 500 project: create objects, load data, and clean

-- 1. Create Database and Schema
CREATE DATABASE IF NOT EXISTS sp500_data;
USE DATABASE sp500_data;
CREATE SCHEMA IF NOT EXISTS finance;
USE SCHEMA finance;

-- 2. Create Tables
CREATE TABLE IF NOT EXISTS finance.companies (
    exchange STRING,
    symbol STRING NOT NULL PRIMARY KEY,
    shortname STRING,
    longname STRING,
    sector STRING,
    industry STRING,
    current_price FLOAT,
    market_cap FLOAT,
    ebitda FLOAT,
    revenue_growth FLOAT,
    city STRING,
    state STRING,
    country STRING,
    fulltime_employees INT,
    long_business_summary STRING,
    weight FLOAT
);

CREATE TABLE IF NOT EXISTS finance.index_data (
    index_date DATE NOT NULL PRIMARY KEY,
    sp500_close FLOAT
);

CREATE TABLE IF NOT EXISTS finance.stock_prices (
    trade_date DATE NOT NULL,
    symbol STRING NOT NULL,
    adj_close FLOAT,
    close FLOAT,
    high FLOAT,
    low FLOAT,
    open FLOAT,
    volume BIGINT,
    CONSTRAINT pk_stock_prices PRIMARY KEY(symbol, trade_date),
    CONSTRAINT fk_symbol FOREIGN KEY(symbol) REFERENCES finance.companies(symbol)
);

-- 3. Create Stage and File Format for CSV ingest
CREATE OR REPLACE STAGE sp500_stage;
CREATE OR REPLACE FILE FORMAT csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  NULL_IF = ('NULL','null','');

-- 4. Load data into tables
COPY INTO finance.companies FROM @sp500_stage/sp500_companies.csv FILE_FORMAT = csv_format;
COPY INTO finance.index_data FROM @sp500_stage/sp500_index.csv FILE_FORMAT = csv_format PATTERN = '.*sp500_index.csv';
COPY INTO finance.stock_prices FROM @sp500_stage/sp500_stocks.csv FILE_FORMAT = csv_format PATTERN = '.*sp500_stocks.csv';

-- 5. Data Cleaning
-- 5.1 Remove companies with NULL revenue_growth
DELETE FROM finance.companies
 WHERE revenue_growth IS NULL;

-- 5.2 Remove stock_prices rows with NULL adj_close
DELETE FROM finance.stock_prices
 WHERE adj_close IS NULL;

-- 5.3 Drop redundant column longname
ALTER TABLE finance.companies DROP COLUMN IF EXISTS longname;

-- 5.4 Remove any stock_prices rows whose symbol is not in companies
DELETE FROM finance.stock_prices
 WHERE symbol NOT IN (SELECT symbol FROM finance.companies);

-- 6. Create sector vs index comparison table
CREATE OR REPLACE TABLE finance.sector_index_compare AS
WITH sector_avg AS (
    SELECT sp.trade_date, c.sector, AVG(sp.close) AS avg_sector_price
    FROM finance.stock_prices sp
    JOIN finance.companies c ON sp.symbol = c.symbol
    GROUP BY sp.trade_date, c.sector
),
index_values AS (
    SELECT index_date, sp500_close FROM finance.index_data
)
SELECT sa.trade_date, sa.sector, sa.avg_sector_price, iv.sp500_close
FROM sector_avg sa
JOIN index_values iv ON sa.trade_date = iv.index_date
ORDER BY sa.trade_date, sa.sector;
