"""Databricks Connect integration — run Spark workloads on remote Databricks serverless compute.

Python logic stays in the Dagster process; all Spark execution happens on a Databricks
serverless cluster via the databricks-connect package.  Results stream back to Dagster.

When to use:
    - You want to keep pipeline logic centralized in Dagster
    - Interactive development with quick iteration
    - Moderate Spark workloads (queries, validation, aggregations)

Not ideal for:
    - Long-running batch jobs that should run independently
    - Workloads that need to outlive the Dagster process

Required env vars:
    DATABRICKS_HOST             - https://dbc-xxxx-yyyy.cloud.databricks.com/
    DATABRICKS_CONNECTION_TOKEN - Personal access token or service-principal token

Demo-safe: set DEMO_MODE=true (or leave DATABRICKS_CONNECTION_TOKEN unset) to run with
synthetic data and no live cluster.

Docs: https://docs.dagster.io/integrations/libraries/databricks/databricks-connect
"""

import os

import dagster as dg
from pyspark.sql import SparkSession

# ---------------------------------------------------------------------------
# Demo flag
# ---------------------------------------------------------------------------

_DEMO_MODE: bool = (
    os.getenv("DEMO_MODE", "false").lower() == "true"
    or not os.getenv("DATABRICKS_CONNECTION_TOKEN")
)

# ---------------------------------------------------------------------------
# Resource — DatabricksSession registered as "spark"
# ---------------------------------------------------------------------------

@dg.resource
def _spark_resource(context) -> SparkSession:
    """Creates the DatabricksSession at run time, not at definition-load time."""
    from databricks.connect import DatabricksSession

    return (
        DatabricksSession.builder.remote(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_CONNECTION_TOKEN"],
        )
        .serverless()
        .getOrCreate()
    )


@dg.definitions
def connect_defs() -> dg.Definitions:
    return dg.Definitions(
        resources={"spark": dg.ResourceDefinition.none_resource() if _DEMO_MODE else _spark_resource},
    )
