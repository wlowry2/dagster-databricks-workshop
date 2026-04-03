"""Data Validation Pipeline — Databricks Connect scenario.

Scene
-----
Your team ingests raw customer records from multiple upstream sources into a Unity
Catalog table every hour. Before any downstream report or ML job is allowed to run,
you need to confirm the data landed correctly — right row count, acceptable null rate.

Why Databricks Connect?
    The validation logic lives in Dagster (where you control the pipeline), but the
    actual Spark queries run on a Databricks serverless cluster against the real table
    in Unity Catalog. Results come back to Dagster so the pipeline can make decisions:
    pass validation → unblock downstream assets; fail → stop the pipeline here.

Pipeline
--------
    raw_customer_table
          ↓
    validated_customer_data   ← Databricks Connect (Spark SQL on serverless cluster)
          ↓
    customer_summary_report
"""

import os

import dagster as dg
from pyspark.sql import SparkSession

_DEMO_MODE = (
    os.getenv("DEMO_MODE", "false").lower() == "true"
    or not os.getenv("DATABRICKS_CONNECTION_TOKEN")
)


@dg.asset(
    group_name="customer_data_pipeline",
    description="Raw customer records landed in Unity Catalog from upstream ingestion.",
    kinds={"databricks"},
)
def raw_customer_table(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Stub representing raw data arriving from your ingestion pipeline.
    In production this would be materialized by your ingest job upstream.
    """
    context.log.info("Raw customer table available at workspace.workshop.customers_raw")
    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text("workspace.workshop.customers_raw"),
            "source": dg.MetadataValue.text("upstream ingestion"),
        }
    )


@dg.asset(
    group_name="customer_data_pipeline",
    description=(
        "Validates row counts and null rates on raw customer data using Spark on Databricks. "
        "Blocks downstream assets if data quality thresholds are not met."
    ),
    kinds={"databricks", "spark"},
    deps=[raw_customer_table],
)
def validated_customer_data(
    context: dg.AssetExecutionContext,
    spark: dg.ResourceParam[SparkSession],
) -> dg.MaterializeResult:
    """Runs validation queries via Databricks Connect.

    Raises if row count is below 1,000 or null rate on email exceeds 5% —
    preventing bad data from ever reaching the downstream report.
    """
    if _DEMO_MODE:
        context.log.info("DEMO_MODE: returning synthetic validation results.")
        return dg.MaterializeResult(
            metadata={
                "row_count": dg.MetadataValue.int(125_000),
                "null_rate": dg.MetadataValue.float(0.02),
                "validation": dg.MetadataValue.text("passed"),
                "mode": dg.MetadataValue.text("demo"),
            }
        )

    df = spark.sql(
        "SELECT COUNT(*) AS cnt, "
        "SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS nulls "
        "FROM workspace.workshop.customers_raw"
    )
    row = df.collect()[0]
    row_count = int(row["cnt"])
    null_rate = float(row["nulls"]) / row_count if row_count > 0 else 0.0

    if row_count < 1_000:
        raise ValueError(f"Row count {row_count:,} is below minimum threshold of 1,000")
    if null_rate > 0.05:
        raise ValueError(f"Null rate {null_rate:.1%} exceeds maximum of 5%")

    context.log.info(f"Validation passed: {row_count:,} rows, {null_rate:.1%} null rate")
    return dg.MaterializeResult(
        metadata={
            "row_count": dg.MetadataValue.int(row_count),
            "null_rate": dg.MetadataValue.float(null_rate),
            "validation": dg.MetadataValue.text("passed"),
            "mode": dg.MetadataValue.text("production"),
        }
    )


@dg.asset(
    group_name="customer_data_pipeline",
    description=(
        "Customer summary report. Only runs after validation passes — "
        "bad data never reaches this step."
    ),
    deps=[validated_customer_data],
)
def customer_summary_report(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Downstream report unblocked only when validated_customer_data succeeds."""
    context.log.info("Generating customer summary report from validated data.")
    return dg.MaterializeResult(
        metadata={
            "report": dg.MetadataValue.text("customer_summary_latest"),
            "status": dg.MetadataValue.text("generated"),
        }
    )
