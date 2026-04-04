"""Clickstream ML Pipeline — Dagster Pipes scenario.

Scene
-----
Your data engineering team has a Databricks notebook that transforms raw clickstream
events into a feature table used for ML model training. The notebook has been running
in Databricks for months and works well — you don't want to rewrite it in Python.

You do want Dagster to orchestrate it: trigger it on a schedule, make sure the ML
training job only runs once the feature table is ready and complete, and surface the
run status and metadata back in Dagster.

Why Dagster Pipes?
    The notebook stays exactly as-is on Databricks. Dagster triggers the job and waits
    for it to complete. If the notebook fails, ml_model_training never runs — no stale
    features fed to the model.

    Note: real-time log streaming and structured metadata reporting require the notebook
    to be submitted with a classic cluster (new_cluster spec). For serverless-only
    workspaces, Dagster triggers the notebook as a pre-existing Databricks Job using
    jobs.run_now() and reads completion status back.

Pipeline
--------
    raw_clickstream_events
          ↓
    clickstream_feature_table   ← Databricks Job (existing notebook triggered via SDK)
          ↓
    ml_model_training

Required env vars:
    DATABRICKS_HOST             - https://dbc-xxxx-yyyy.cloud.databricks.com/
    DATABRICKS_CONNECTION_TOKEN - Personal access token
    DATABRICKS_NOTEBOOK_JOB_ID  - Databricks Job ID for the feature engineering notebook
                                   Workflows > click your job > Job ID in the URL
"""

import os

import dagster as dg

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
        "Triggers the existing Databricks feature engineering notebook and waits for completion. "
        "Blocks ML training until the feature table is ready."
    ),
    kinds={"databricks", "notebook"},
    deps=[raw_clickstream_events],
)
def clickstream_feature_table(
    context: dg.AssetExecutionContext,
) -> dg.MaterializeResult:
    """Triggers the feature engineering notebook as a Databricks Job and waits for it to finish.

    If the job fails, ml_model_training never runs — no stale features fed to the model.
    Set DATABRICKS_NOTEBOOK_JOB_ID to the Job ID of the feature engineering notebook in
    your Databricks workspace (Workflows > click job > ID in the URL).
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

    from databricks.sdk import WorkspaceClient

    host = os.environ["DATABRICKS_HOST"]
    token = os.environ["DATABRICKS_CONNECTION_TOKEN"]
    job_id = int(os.environ["DATABRICKS_NOTEBOOK_JOB_ID"])

    client = WorkspaceClient(host=host, token=token)

    context.log.info(f"Triggering feature engineering notebook job {job_id}")
    run = client.jobs.run_now(job_id=job_id).result()
    run_url = f"{host}/#job/{job_id}/run/{run.run_id}"

    context.log.info(f"Job completed — run_id={run.run_id}, state={run.state.result_state}")
    return dg.MaterializeResult(
        metadata={
            "job_id": dg.MetadataValue.text(str(job_id)),
            "run_id": dg.MetadataValue.text(str(run.run_id)),
            "run_url": dg.MetadataValue.url(run_url),
            "output_table": dg.MetadataValue.text("workspace.workshop.clickstream_features"),
            "status": dg.MetadataValue.text(str(run.state.result_state)),
        }
    )


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
