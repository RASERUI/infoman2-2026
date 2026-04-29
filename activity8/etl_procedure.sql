-- ============================================================
-- etl_procedure.sql
-- ETL Stored Procedure for Coffee Chain Sales Data Warehouse
-- Procedure: dw.run_sales_etl()
-- ============================================================

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
        EXTRACT(DAY   FROM s.txn_date)::SMALLINT,
        EXTRACT(MONTH FROM s.txn_date)::SMALLINT,
        EXTRACT(YEAR  FROM s.txn_date)::SMALLINT,
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
    -- Reject rows with qty <= 0, unit_price <= 0, or missing FKs
    -- ----------------------------------------------------------
    SELECT COUNT(*)
    INTO v_bad_rows
    FROM public.sales_txn s
    WHERE s.id > v_max_txn_id
      AND (
            s.qty        <= 0
         OR s.unit_price <= 0
         OR s.customer_id IS NULL
         OR s.product_id  IS NULL
         OR s.branch_id   IS NULL
         OR s.txn_date    IS NULL
      );

    IF v_bad_rows > 0 THEN
        RAISE NOTICE 'Data quality warning: % row(s) failed validation and will be skipped.', v_bad_rows;
    END IF;

    -- ----------------------------------------------------------
    -- STEP 7: Load fact_sales (incremental, quality-filtered)
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
        s.id                        AS txn_id,
        dd.date_key,
        dc.customer_key,
        dp.product_key,
        db.branch_key,
        s.qty,
        s.unit_price,
        (s.qty * s.unit_price)      AS total_amount
    FROM public.sales_txn s
    -- resolve surrogate keys
    JOIN dw.dim_date     dd ON dd.full_date  = s.txn_date
    JOIN dw.dim_customer dc ON dc.source_id  = s.customer_id
    JOIN dw.dim_product  dp ON dp.source_id  = s.product_id
    JOIN dw.dim_branch   db ON db.source_id  = s.branch_id
    -- incremental filter: only new transactions
    WHERE s.id > v_max_txn_id
    -- data quality filter: only valid rows
      AND s.qty        > 0
      AND s.unit_price > 0
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
        -- Log FAIL with error message
        INSERT INTO dw.etl_log (run_ts, status, rows_loaded, error_message)
        VALUES (NOW(), 'FAIL', 0, SQLERRM);

        RAISE NOTICE 'ETL failed: %', SQLERRM;
END;
$$;

-- ============================================================
-- Sample Execution
-- ============================================================
CALL dw.run_sales_etl();

-- ============================================================
-- View ETL Log Entries
-- ============================================================
SELECT run_ts, status, rows_loaded, error_message
FROM dw.etl_log
ORDER BY run_ts DESC;
