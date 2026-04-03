-- =============================================================================
-- Pipeline 2: Clickstream ML (Dagster Pipes)
-- Creates main.events.clickstream_raw with 5,000 rows of fake event data,
-- and the main.features schema where the notebook will write its output.
--
-- Used by: clickstream_feature_table asset (clickstream_ml_pipeline.py)
-- Run this in a Databricks SQL worksheet before running the pipeline.
--
-- Note: adjust catalog/schema if your workspace uses a different default catalog.
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS main.events;
CREATE SCHEMA IF NOT EXISTS main.features;  -- notebook writes feature table here

CREATE OR REPLACE TABLE main.events.clickstream_raw (
  event_id         STRING,
  user_id          STRING,
  event_type       STRING,
  page_url         STRING,
  event_timestamp  TIMESTAMP
);

INSERT INTO main.events.clickstream_raw
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
FROM main.events.clickstream_raw;

SELECT event_type, COUNT(*) AS cnt
FROM main.events.clickstream_raw
GROUP BY event_type
ORDER BY cnt DESC;
