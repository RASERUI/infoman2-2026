# Activity 8 Answer Template

## Part 1: Star Schema Design

### 1. Fact Table Grain

The grain of the fact table is **one row per transaction line item per day**. Each row represents a single product sold in a transaction at a specific branch on a specific date.

### 2. Fact Measures

- `qty` — number of units sold in the transaction line
- `unit_price` — price per unit at the time of sale
- `total_amount` — derived measure: `qty × unit_price`

### 3. Dimension Tables and Attributes

- `dim_date`: `date_key` (PK), `full_date`, `day`, `month`, `year`, `quarter`
- `dim_customer`: `customer_key` (PK), `source_id`, `full_name`, `region_code`
- `dim_product`: `product_key` (PK), `source_id`, `product_name`, `category`, `unit_price`
- `dim_branch`: `branch_key` (PK), `source_id`, `branch_name`, `city`, `region`

### 4. Relationship Summary

`fact_sales` connects to all four dimensions via surrogate foreign keys:

- `fact_sales.date_key` → `dim_date.date_key`
- `fact_sales.customer_key` → `dim_customer.customer_key`
- `fact_sales.product_key` → `dim_product.product_key`
- `fact_sales.branch_key` → `dim_branch.branch_key`

Each dimension also retains a `source_id` column holding the original OLTP business key, used during ETL to look up and resolve surrogate keys.

---

## Part 2: Warehouse DDL

```sql
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
```

---

## Part 3: ETL Procedure

### 1. Procedure Code

```sql
CREATE OR REPLACE PROCEDURE dw.run_sales_etl()
LANGUAGE plpgsql
AS $$
DECLARE
    v_rows_loaded   INT := 0;
    v_max_txn_id    INT;
    v_bad_rows      INT;
BEGIN

    -- ----------------------------------------------------------
    -- STEP 1: Upsert dim_date
    -- Insert any new dates found in sales_txn not yet in dim_date
    -- ----------------------------------------------------------
    INSERT INTO dw.dim_date (full_date, day, month, year, quarter)
    SELECT DISTINCT
        s.txn_date,
        EXTRACT(DAY     FROM s.txn_date)::SMALLINT,
        EXTRACT(MONTH   FROM s.txn_date)::SMALLINT,
        EXTRACT(YEAR    FROM s.txn_date)::SMALLINT,
        EXTRACT(QUARTER FROM s.txn_date)::SMALLINT
    FROM public.sales_txn s
    ON CONFLICT (full_date) DO NOTHING;

    -- ----------------------------------------------------------
    -- STEP 2: Upsert dim_customer
    -- ----------------------------------------------------------
    INSERT INTO dw.dim_customer (source_id, full_name, region_code)
    SELECT
        c.id,
        c.full_name,
        c.region_code
    FROM public.customers c
    ON CONFLICT (source_id) DO UPDATE
        SET full_name   = EXCLUDED.full_name,
            region_code = EXCLUDED.region_code;

    -- ----------------------------------------------------------
    -- STEP 3: Upsert dim_product
    -- ----------------------------------------------------------
    INSERT INTO dw.dim_product (source_id, product_name, category, unit_price)
    SELECT
        p.id,
        p.product_name,
        p.category,
        p.unit_price
    FROM public.products p
    ON CONFLICT (source_id) DO UPDATE
        SET product_name = EXCLUDED.product_name,
            category     = EXCLUDED.category,
            unit_price   = EXCLUDED.unit_price;

    -- ----------------------------------------------------------
    -- STEP 4: Upsert dim_branch
    -- ----------------------------------------------------------
    INSERT INTO dw.dim_branch (source_id, branch_name, city, region)
    SELECT
        b.id,
        b.branch_name,
        b.city,
        b.region
    FROM public.branches b
    ON CONFLICT (source_id) DO UPDATE
        SET branch_name = EXCLUDED.branch_name,
            city        = EXCLUDED.city,
            region      = EXCLUDED.region;

    -- ----------------------------------------------------------
    -- STEP 5: Determine incremental boundary
    -- Only load sales_txn rows with id > max already loaded txn_id
    -- ----------------------------------------------------------
    SELECT COALESCE(MAX(txn_id), 0)
    INTO v_max_txn_id
    FROM dw.fact_sales;

    -- ----------------------------------------------------------
    -- STEP 6: Data quality check
    -- Count rows that fail validation (will be skipped)
    -- ----------------------------------------------------------
    SELECT COUNT(*)
    INTO v_bad_rows
    FROM public.sales_txn s
    WHERE s.id > v_max_txn_id
      AND (
            s.qty         <= 0
         OR s.unit_price  <= 0
         OR s.customer_id IS NULL
         OR s.product_id  IS NULL
         OR s.branch_id   IS NULL
         OR s.txn_date    IS NULL
      );

    IF v_bad_rows > 0 THEN
        RAISE NOTICE 'Data quality warning: % row(s) failed validation and will be skipped.', v_bad_rows;
    END IF;

    -- ----------------------------------------------------------
    -- STEP 7: Load fact_sales (incremental + quality-filtered)
    -- Join source rows to dimensions to resolve surrogate keys
    -- ----------------------------------------------------------
    INSERT INTO dw.fact_sales (
        txn_id,
        date_key,
        customer_key,
        product_key,
        branch_key,
        qty,
        unit_price,
        total_amount
    )
    SELECT
        s.id                    AS txn_id,
        dd.date_key,
        dc.customer_key,
        dp.product_key,
        db.branch_key,
        s.qty,
        s.unit_price,
        (s.qty * s.unit_price)  AS total_amount
    FROM public.sales_txn s
    JOIN dw.dim_date     dd ON dd.full_date = s.txn_date
    JOIN dw.dim_customer dc ON dc.source_id = s.customer_id
    JOIN dw.dim_product  dp ON dp.source_id = s.product_id
    JOIN dw.dim_branch   db ON db.source_id = s.branch_id
    WHERE s.id > v_max_txn_id
      AND s.qty         > 0
      AND s.unit_price  > 0
      AND s.customer_id IS NOT NULL
      AND s.product_id  IS NOT NULL
      AND s.branch_id   IS NOT NULL
      AND s.txn_date    IS NOT NULL;

    GET DIAGNOSTICS v_rows_loaded = ROW_COUNT;

    -- ----------------------------------------------------------
    -- STEP 8: Log SUCCESS
    -- ----------------------------------------------------------
    INSERT INTO dw.etl_log (run_ts, status, rows_loaded, error_message)
    VALUES (NOW(), 'SUCCESS', v_rows_loaded, NULL);

    RAISE NOTICE 'ETL completed successfully. Rows loaded: %', v_rows_loaded;

EXCEPTION
    WHEN OTHERS THEN
        INSERT INTO dw.etl_log (run_ts, status, rows_loaded, error_message)
        VALUES (NOW(), 'FAIL', 0, SQLERRM);

        RAISE NOTICE 'ETL failed: %', SQLERRM;
END;
$$;
```

### 2. Procedure Execution

```sql
CALL dw.run_sales_etl();
```

### 3. ETL Log Output

```sql
SELECT * FROM dw.etl_log ORDER BY run_ts DESC;
```

```txt
            run_ts             | status | rows_loaded | error_message
------------------------------+--------+-------------+---------------
 2026-04-29 14:54:02.360858   | SUCCESS|           4 |
(1 row)
```

---

## Part 4: Analytical Queries

### Query 1: Monthly Revenue by Branch Region

```sql
SELECT b.region, SUM(f.total_amount) AS revenue
FROM dw.fact_sales f
JOIN dw.dim_branch b ON f.branch_key = b.branch_key
GROUP BY b.region
ORDER BY revenue DESC;
```

Interpretation: The Northeast (NE) region generates the highest revenue at $12.00, followed by the Northwest ($8.25) and Southeast ($3.00), indicating that NE branches have stronger sales performance and may warrant increased inventory or staffing.

### Query 2: Top 5 Products by Total Revenue

```sql
SELECT p.product_name, SUM(f.total_amount) AS revenue
FROM dw.fact_sales f
JOIN dw.dim_product p ON f.product_key = p.product_key
GROUP BY p.product_name
ORDER BY revenue DESC
LIMIT 5;
```

Interpretation: Ranks products by total revenue generated across all branches, helping management identify which items drive the most sales and should be prioritized for promotions, restocking, and menu placement.

### Query 3: Customer Region Contribution to Sales

```sql
SELECT c.region_code, SUM(f.total_amount) AS revenue,
       ROUND(SUM(f.total_amount) / SUM(SUM(f.total_amount)) OVER () * 100, 2) AS pct_of_total
FROM dw.fact_sales f
JOIN dw.dim_customer c ON f.customer_key = c.customer_key
GROUP BY c.region_code
ORDER BY revenue DESC;
```

Interpretation: Shows each customer region's share of total sales revenue as a percentage, revealing which customer segments contribute most to overall business performance and where targeted marketing efforts would have the greatest impact.
