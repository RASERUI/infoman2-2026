-- ============================================================
-- warehouse_setup.sql
-- Star Schema DDL for Coffee Chain Sales Data Warehouse
-- Schema: dw
-- ============================================================

-- ------------------------------------------------------------
-- 1. Create Schema
-- ------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS dw;

-- ------------------------------------------------------------
-- 2. Dimension Tables
-- ------------------------------------------------------------

-- dim_date: one row per calendar date
CREATE TABLE IF NOT EXISTS dw.dim_date (
    date_key    SERIAL          PRIMARY KEY,
    full_date   DATE            NOT NULL UNIQUE,
    day         SMALLINT        NOT NULL,
    month       SMALLINT        NOT NULL,
    year        SMALLINT        NOT NULL,
    quarter     SMALLINT        NOT NULL
);

-- dim_customer: one row per customer
CREATE TABLE IF NOT EXISTS dw.dim_customer (
    customer_key    SERIAL          PRIMARY KEY,
    source_id       INT             NOT NULL UNIQUE,   -- maps to public.customers.id
    full_name       VARCHAR(200)    NOT NULL,
    region_code     VARCHAR(10)
);

-- dim_product: one row per product
CREATE TABLE IF NOT EXISTS dw.dim_product (
    product_key     SERIAL          PRIMARY KEY,
    source_id       INT             NOT NULL UNIQUE,   -- maps to public.products.id
    product_name    VARCHAR(200)    NOT NULL,
    category        VARCHAR(100),
    unit_price      NUMERIC(10,2)   NOT NULL
);

-- dim_branch: one row per branch
CREATE TABLE IF NOT EXISTS dw.dim_branch (
    branch_key      SERIAL          PRIMARY KEY,
    source_id       INT             NOT NULL UNIQUE,   -- maps to public.branches.id
    branch_name     VARCHAR(200)    NOT NULL,
    city            VARCHAR(100),
    region          VARCHAR(50)
);

-- ------------------------------------------------------------
-- 3. Fact Table
-- ------------------------------------------------------------

-- fact_sales: grain = one row per transaction line item per day
-- Each row = one product sold in one transaction at one branch on one date
CREATE TABLE IF NOT EXISTS dw.fact_sales (
    fact_id         SERIAL          PRIMARY KEY,
    txn_id          INT             NOT NULL,           -- source transaction id (for incremental load)
    date_key        INT             NOT NULL REFERENCES dw.dim_date(date_key),
    customer_key    INT             NOT NULL REFERENCES dw.dim_customer(customer_key),
    product_key     INT             NOT NULL REFERENCES dw.dim_product(product_key),
    branch_key      INT             NOT NULL REFERENCES dw.dim_branch(branch_key),
    qty             INT             NOT NULL,
    unit_price      NUMERIC(10,2)   NOT NULL,
    total_amount    NUMERIC(12,2)   NOT NULL            -- qty × unit_price
);

-- ------------------------------------------------------------
-- 4. ETL Log Table
-- ------------------------------------------------------------

CREATE TABLE IF NOT EXISTS dw.etl_log (
    log_id          SERIAL          PRIMARY KEY,
    run_ts          TIMESTAMP       NOT NULL DEFAULT NOW(),
    status          VARCHAR(10)     NOT NULL,           -- 'SUCCESS' or 'FAIL'
    rows_loaded     INT,
    error_message   TEXT
);

-- ------------------------------------------------------------
-- 5. Indexes
-- ------------------------------------------------------------

-- Analytical filtering: most queries filter or join on date and branch
CREATE INDEX IF NOT EXISTS idx_fact_sales_date_key
    ON dw.fact_sales(date_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_branch_key
    ON dw.fact_sales(branch_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_product_key
    ON dw.fact_sales(product_key);

CREATE INDEX IF NOT EXISTS idx_fact_sales_customer_key
    ON dw.fact_sales(customer_key);

-- Incremental load: quickly find max loaded txn_id
CREATE INDEX IF NOT EXISTS idx_fact_sales_txn_id
    ON dw.fact_sales(txn_id);

-- Dimension lookup by source_id (used during ETL upserts)
CREATE INDEX IF NOT EXISTS idx_dim_customer_source_id
    ON dw.dim_customer(source_id);

CREATE INDEX IF NOT EXISTS idx_dim_product_source_id
    ON dw.dim_product(source_id);

CREATE INDEX IF NOT EXISTS idx_dim_branch_source_id
    ON dw.dim_branch(source_id);

CREATE INDEX IF NOT EXISTS idx_dim_date_full_date
    ON dw.dim_date(full_date);
