-- =============================================================================
-- Pipeline 2: Clickstream ML (Dagster Pipes)
-- Creates workspace.workshop.clickstream_raw with 5,000 rows of fake event data.
-- The notebook will also write its output to workspace.workshop.clickstream_features.
--
-- Used by: clickstream_feature_table asset (clickstream_ml_pipeline.py)
-- Run this in a Databricks SQL worksheet before running the pipeline.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS workspace.workshop;

CREATE OR REPLACE TABLE workspace.workshop.clickstream_raw (
  event_id         STRING,
  user_id          STRING,
  event_type       STRING,
  page_url         STRING,
  event_timestamp  TIMESTAMP
);

INSERT INTO workspace.workshop.clickstream_raw
WITH ids AS (SELECT explode(sequence(1, 5000)) AS id)
SELECT
  concat('evt_', lpad(cast(id AS STRING), 5, '0')) AS event_id,

  -- 200 unique users cycling through
  concat('user_', lpad(cast(mod(id, 200) + 1 AS STRING), 3, '0')) AS user_id,

  -- weighted toward page_view (most common), rare purchases
  element_at(array(
    'page_view', 'page_view', 'page_view',
    'click',     'click',
    'signup',
    'purchase'
  ), (mod(id, 7) + 1)) AS event_type,

  element_at(array(
    '/home',     '/pricing',  '/features',
    '/blog',     '/about',
    '/register',
    '/checkout'
  ), (mod(id, 7) + 1)) AS page_url,

  -- spread events over the last 30 days
  current_timestamp() - make_interval(0, 0, 0, mod(id, 30)) - make_interval(0, 0, 0, 0, 0, mod(id, 1440)) AS event_timestamp

FROM ids;

-- Verify
SELECT
  COUNT(*)              AS total_events,
  COUNT(DISTINCT user_id)  AS unique_users,
  COUNT(DISTINCT event_type) AS event_types
FROM workspace.workshop.clickstream_raw;

SELECT event_type, COUNT(*) AS cnt
FROM workspace.workshop.clickstream_raw
GROUP BY event_type
ORDER BY cnt DESC;
