# Databricks notebook source
# =============================================================================
# Feature Engineering Notebook — Dagster Pipes
#
# This notebook is triggered by Dagster via the Pipes protocol.
# It reads raw clickstream events, builds user-level features,
# writes the feature table, and reports metadata back to Dagster.
#
# Setup:
#   1. Upload this file to your Databricks workspace
#   2. Set DATABRICKS_NOTEBOOK_PATH in your .env to its workspace path
#      e.g. /Users/you@company.com/feature_engineering
#   3. Run the pipeline from Dagster — it will submit this notebook as a job
#
# Install dagster-pipes on the cluster (or add to notebook libraries):
#   %pip install dagster-pipes
# =============================================================================

# COMMAND ----------

%pip install dagster-pipes

# COMMAND ----------

from dagster_pipes import (
    PipesDbfsContextLoader,
    PipesDbfsMessageWriter,
    open_dagster_pipes,
)

# COMMAND ----------

with open_dagster_pipes(
    context_loader=PipesDbfsContextLoader(),
    message_writer=PipesDbfsMessageWriter(),
) as pipes:

    # ── Read raw clickstream events ────────────────────────────────────────
    raw = spark.sql("SELECT * FROM main.events.clickstream_raw")  # noqa: F821
    pipes.log.info(f"Read {raw.count():,} raw events")

    # ── Build user-level features ──────────────────────────────────────────
    features = spark.sql("""  # noqa: F821
        SELECT
            user_id,
            COUNT(*)                                                        AS total_events,
            SUM(CASE WHEN event_type = 'page_view'  THEN 1 ELSE 0 END)    AS page_views,
            SUM(CASE WHEN event_type = 'click'      THEN 1 ELSE 0 END)    AS clicks,
            SUM(CASE WHEN event_type = 'signup'     THEN 1 ELSE 0 END)    AS signups,
            SUM(CASE WHEN event_type = 'purchase'   THEN 1 ELSE 0 END)    AS purchases,
            COUNT(DISTINCT page_url)                                        AS unique_pages,
            MIN(event_timestamp)                                            AS first_seen,
            MAX(event_timestamp)                                            AS last_seen
        FROM main.events.clickstream_raw
        GROUP BY user_id
    """)

    # ── Write feature table ────────────────────────────────────────────────
    features.write.mode("overwrite").saveAsTable("main.features.clickstream_features")

    row_count    = features.count()
    feature_cols = len(features.columns) - 1  # exclude user_id

    pipes.log.info(f"Wrote {row_count} users, {feature_cols} features to main.features.clickstream_features")

    # ── Report back to Dagster ─────────────────────────────────────────────
    # This metadata appears on the asset in the Dagster UI
    pipes.report_asset_materialization(
        metadata={
            "row_count":     {"raw_value": row_count,                                "type": "int"},
            "feature_count": {"raw_value": feature_cols,                             "type": "int"},
            "output_table":  {"raw_value": "main.features.clickstream_features",     "type": "text"},
            "source_table":  {"raw_value": "main.events.clickstream_raw",            "type": "text"},
        }
    )
