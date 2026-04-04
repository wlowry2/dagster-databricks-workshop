# Databricks notebook source
# =============================================================================
# Feature Engineering Notebook
#
# Reads raw clickstream events, builds user-level features, and writes the
# feature table to workspace.workshop.clickstream_features.
#
# Run this notebook manually in Databricks or trigger it via Dagster as a Job.
# =============================================================================

# COMMAND ----------

raw = spark.sql("SELECT * FROM workspace.workshop.clickstream_raw")
print(f"Read {raw.count():,} raw events")

# COMMAND ----------

features = spark.sql(
    """
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
    FROM workspace.workshop.clickstream_raw
    GROUP BY user_id
    """
)

# COMMAND ----------

features.write.mode("overwrite").saveAsTable("workspace.workshop.clickstream_features")

row_count    = features.count()
feature_cols = len(features.columns) - 1  # exclude user_id

print(f"Wrote {row_count} users, {feature_cols} features to workspace.workshop.clickstream_features")

# COMMAND ----------

display(spark.sql("SELECT * FROM workspace.workshop.clickstream_features LIMIT 10"))
