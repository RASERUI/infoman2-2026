# Lab Activity: Star Schema Design and ETL Pipeline Implementation

## Objective

This activity applies Week 7 and Week 8 concepts by requiring you to:

1. Design a Star Schema from an OLTP-style business scenario.
2. Build a PostgreSQL ETL pipeline using PL/pgSQL.
3. Validate data quality and produce analytical outputs from your warehouse.

---

## Scenario

You are the data engineering team for a multi-branch coffee chain. Branch systems store daily transactions in OLTP tables. Management wants a small data warehouse for analytics.

Your task is to build and load a Sales Star Schema and generate BI-ready queries.

---

## Source Tables (OLTP)

Assume these tables exist in the `public` schema:

```
customers(id, full_name, region_code)
products(id, product_name, category, unit_price)
branches(id, branch_name, city, region)
sales_txn(id, txn_date, customer_id, product_id, branch_id, qty, unit_price)
```

---

## Part 1: Star Schema Design (5 points)

Design a star schema in `dw` schema with the following requirements:

- Define the grain of the fact table.
- Create dimensions: `dim_date`, `dim_customer`, `dim_product`, `dim_branch`
- Create one fact table: `fact_sales`
- Use surrogate keys for dimensions.
- Keep source business keys as `source_id` columns in dimensions for ETL lookup.

**Deliverable:** A short write-up describing fact table grain, measures, dimension attributes, and relationships.

---

## Part 2: Warehouse DDL Setup (4 points)

Write SQL to create:

- `dw` schema
- All dimension and fact tables
- `dw.etl_log` table (`run_ts`, `status`, `rows_loaded`, `error_message`)
- Constraints and indexes for ETL lookup and query performance

**Minimum technical expectations:**
- `ON CONFLICT`-ready uniqueness on `source_id` in dimensions
- Foreign keys from fact table to dimensions
- At least one index that helps analytical filtering (e.g., `date_key` or `branch_key`)

---

## Part 3: ETL Procedure with PL/pgSQL (8 points)

Create a stored procedure: `dw.run_sales_etl()`

**Required ETL behavior:**
- Load dimensions first using upsert pattern (`INSERT ... ON CONFLICT ... DO UPDATE`)
- Load fact table by joining source rows to dimensions to resolve surrogate keys
- Data quality checks before fact load: `qty > 0`, `unit_price > 0`, no null required foreign references
- Incremental loading: load only rows from `sales_txn` not yet loaded to `fact_sales`
- Logging: insert `SUCCESS` row with row count on completion; catch exception and insert `FAIL` with `SQLERRM`

**Deliverable:** Full SQL code of procedure, one sample `CALL`, and a query showing ETL log entries.

---

## Part 4: Analytical Queries (3 points)

Write and run at least three OLAP-style queries:

1. Monthly revenue by branch region
2. Top 5 products by total revenue
3. Customer-region contribution to total sales

For each query, include the SQL statement and a brief interpretation (1–2 sentences).

---

## Submission Format

Submit one folder `activity8` containing:

- `star_schema_etl_class_activity.md` (this file)
- `answer_template.md` (completed answers)
- `warehouse_setup.sql` (DDL + indexes)
- `etl_procedure.sql` (procedure + sample call)

---

## Grading Rubric (20 Points)

| Criteria | Excellent | Satisfactory | Needs Improvement | Points |
|---|---|---|---|---|
| Star Schema Design | Clear grain, correct fact/dim split, proper surrogate key strategy | Minor modeling gaps but mostly correct | Confused grain or incorrect fact/dim design | 5 |
| Warehouse DDL | Complete schema, keys, constraints, and useful indexes | Mostly complete with minor missing constraints/indexes | Incomplete or invalid DDL | 4 |
| ETL Procedure (PL/pgSQL) | Correct upserts, key lookups, data-quality checks, incremental load, and logging | Procedure runs but misses one major required behavior | ETL logic largely incorrect or non-functional | 8 |
| Analytical Queries | 3 meaningful OLAP queries with correct SQL and interpretation | 3 queries provided with limited analysis depth | Fewer than 3 or incorrect queries | 3 |

---

## Notes

- Use the Week 8 ETL handout pattern for upserts, key lookups, and error logging.
- Favor readability and correctness over over-engineering.
- You may add assumptions if clearly documented.
