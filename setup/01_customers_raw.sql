-- =============================================================================
-- Pipeline 1: Customer Data Validation (Databricks Connect)
-- Creates workspace.workshop.customers_raw with 1,500 rows of fake customer data.
--
-- Used by: validated_customer_data asset (data_validation_pipeline.py)
-- Run this in a Databricks SQL worksheet before running the pipeline.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.workshop;

CREATE OR REPLACE TABLE workspace.workshop.customers_raw (
  customer_id  STRING,
  name         STRING,
  email        STRING,   -- ~3% null to demonstrate null-rate validation
  status       STRING,
  created_at   TIMESTAMP
);

INSERT INTO workspace.workshop.customers_raw
WITH ids AS (SELECT explode(sequence(1, 1500)) AS id)
SELECT
  concat('cust_', lpad(cast(id AS STRING), 4, '0')) AS customer_id,

  element_at(array(
    'Alice Johnson', 'Bob Smith',    'Carol White',  'Dan Brown',   'Eve Davis',
    'Frank Miller',  'Grace Lee',    'Henry Wilson', 'Iris Chen',   'Jack Taylor',
    'Karen Hall',    'Leo Martinez', 'Mia Thompson', 'Noah Garcia', 'Olivia Clark'
  ), (mod(id, 15) + 1)) AS name,

  -- ~3% null rate (every 33rd row) — well under the 5% threshold in the asset
  CASE
    WHEN mod(id, 33) = 0 THEN NULL
    ELSE concat(
      lower(element_at(array(
        'alice','bob','carol','dan','eve','frank','grace',
        'henry','iris','jack','karen','leo','mia','noah','olivia'
      ), (mod(id, 15) + 1))),
      cast(id AS STRING),
      '@example.com'
    )
  END AS email,

  CASE WHEN mod(id, 5) = 0 THEN 'inactive' ELSE 'active' END AS status,

  current_timestamp() - make_interval(0, 0, 0, mod(id, 90)) AS created_at

FROM ids;

-- Verify
SELECT
  COUNT(*)                                                      AS total_rows,
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END)               AS null_emails,
  round(SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) / COUNT(*) * 100, 2) AS null_pct,
  COUNT(DISTINCT status)                                        AS status_values
FROM workspace.workshop.customers_raw;
