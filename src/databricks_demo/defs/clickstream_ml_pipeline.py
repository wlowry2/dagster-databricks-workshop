"""Clickstream ML Pipeline — Dagster Pipes scenario.

Scene
-----
Your data engineering team has a Databricks notebook that transforms raw clickstream
events into a feature table used for ML model training. The notebook has been running
in Databricks for months and works well — you don't want to rewrite it in Python.

You do want Dagster to orchestrate it: trigger it on a schedule, stream its logs back
in real time, and make sure the ML training job only runs once the feature table is
ready and complete.

Why Dagster Pipes?
    The notebook stays exactly as-is on Databricks. Dagster submits the run, streams
    logs back as the notebook executes, and receives structured metadata (row counts,
    feature stats) when it finishes. If the notebook fails, ml_model_training never
    runs — no stale features fed to the model.

Pipeline
--------
    raw_clickstream_events
          ↓
    clickstream_feature_table   ← Dagster Pipes (existing Databricks notebook)
          ↓
    ml_model_training
"""

import os

import dagster as dg
from databricks.sdk.service import jobs
from dagster_databricks import PipesDatabricksClient

_DEMO_MODE = (
    os.getenv("DEMO_MODE", "false").lower() == "true"
    or not os.getenv("DATABRICKS_CONNECTION_TOKEN")
)


@dg.asset(
    group_name="clickstream_ml_pipeline",
    description="Raw clickstream events arriving from streaming ingestion (Kafka/Kinesis).",
    kinds={"databricks"},
)
def raw_clickstream_events(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Stub representing raw clickstream data from your streaming pipeline.
    In production this would be materialized by your Kafka or Kinesis consumer.
    """
    context.log.info("Raw clickstream events available at workspace.workshop.clickstream_raw")
    return dg.MaterializeResult(
        metadata={
            "table": dg.MetadataValue.text("workspace.workshop.clickstream_raw"),
            "source": dg.MetadataValue.text("kinesis ingestion"),
        }
    )


@dg.asset(
    group_name="clickstream_ml_pipeline",
    description=(
        "Runs the existing Databricks feature engineering notebook via Pipes. "
        "Streams logs back to Dagster in real time. Blocks ML training until complete."
    ),
    kinds={"databricks", "notebook"},
    deps=[raw_clickstream_events],
)
def clickstream_feature_table(
    context: dg.AssetExecutionContext,
    pipes_databricks: PipesDatabricksClient,
) -> dg.MaterializeResult:
    """Triggers the existing feature engineering notebook via Dagster Pipes.

    The notebook runs as-is on Databricks — no rewrite needed. It must call
    open_dagster_pipes() and report_asset_materialization() to send structured
    metadata back. See README for the required notebook boilerplate.
    """
    if _DEMO_MODE:
        context.log.info("DEMO_MODE: returning synthetic feature table results.")
        return dg.MaterializeResult(
            metadata={
                "feature_count": dg.MetadataValue.int(47),
                "row_count": dg.MetadataValue.int(2_400_000),
                "table": dg.MetadataValue.text("workspace.workshop.clickstream_features"),
                "mode": dg.MetadataValue.text("demo"),
            }
        )

    notebook_path = os.environ["DATABRICKS_NOTEBOOK_PATH"]
    task = jobs.SubmitTask.from_dict(
        {
            "task_key": "feature_engineering",
            "new_cluster": {
                "spark_version": "15.4.x-scala2.12",
                "node_type_id": "i3.xlarge",
                "num_workers": 4,
                "cluster_log_conf": {
                    "dbfs": {"destination": "dbfs:/cluster-logs/feature-engineering"},
                },
            },
            "libraries": [{"pypi": {"package": "dagster-pipes"}}],
            "notebook_task": {"notebook_path": notebook_path},
        }
    )

    return pipes_databricks.run(
        task=task,
        context=context,
        extras={
            "run_date": str(context.partition_key) if context.has_partition_key else "latest"
        },
    ).get_materialize_result()


@dg.asset(
    group_name="clickstream_ml_pipeline",
    description=(
        "Trains the churn prediction model on the latest features. "
        "Only runs after clickstream_feature_table succeeds — never trains on stale data."
    ),
    deps=[clickstream_feature_table],
)
def ml_model_training(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Downstream ML training job, unblocked only after feature engineering completes."""
    context.log.info("Training churn prediction model on latest clickstream features.")
    return dg.MaterializeResult(
        metadata={
            "model": dg.MetadataValue.text("churn_prediction_v2"),
            "status": dg.MetadataValue.text("trained"),
        }
    )
