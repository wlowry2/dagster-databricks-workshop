"""Dagster Pipes integration — orchestrate Databricks scripts/notebooks with structured I/O.

Dagster submits a one-time run to Databricks.  The script or notebook uses dagster-pipes
to stream logs and report structured asset metadata back in real time.

When to use:
    - Your team already has Databricks scripts or notebooks doing meaningful work
    - You want Dagster to orchestrate them and receive structured output (counts, schema,
      validation results) without rewriting logic in Python
    - You need full control over job submission and real-time log forwarding

The script/notebook side (run on Databricks) must call open_dagster_pipes().
See the "Notebook setup" section in README.md for the required boilerplate.

Required env vars:
    DATABRICKS_HOST             - https://dbc-xxxx-yyyy.cloud.databricks.com/
    DATABRICKS_CONNECTION_TOKEN - Personal access token or service-principal token
    DATABRICKS_NOTEBOOK_PATH    - Absolute notebook path, e.g. /Users/you@co.com/my_notebook

Optional:
    DATABRICKS_CLUSTER_ID       - Existing cluster ID; omit to launch a new serverless cluster

Demo-safe: set DEMO_MODE=true (or leave DATABRICKS_CONNECTION_TOKEN unset) to return
synthetic metadata without touching Databricks.

Docs: https://docs.dagster.io/integrations/libraries/databricks/dagster-databricks

---- Notebook-side boilerplate ------------------------------------------------

For a Python script on DBFS (spark_python_task):

    from dagster_pipes import PipesDbfsContextLoader, PipesDbfsMessageWriter, open_dagster_pipes

    with open_dagster_pipes(
        context_loader=PipesDbfsContextLoader(),
        message_writer=PipesDbfsMessageWriter(),
    ) as pipes:
        value = pipes.get_extra("some_parameter")
        pipes.log.info(f"Running with some_parameter={value}")
        # ... your computation ...
        pipes.report_asset_materialization(
            metadata={"row_count": {"raw_value": 99_000, "type": "int"}},
            data_version="alpha",
        )

For a serverless notebook using Unity Catalog Volumes:

    from dagster_pipes import (
        PipesDatabricksNotebookWidgetsParamsLoader,
        PipesUnityCatalogVolumesContextLoader,
        PipesUnityCatalogVolumesMessageWriter,
        open_dagster_pipes,
    )

    with open_dagster_pipes(
        context_loader=PipesUnityCatalogVolumesContextLoader(),
        message_writer=PipesUnityCatalogVolumesMessageWriter(),
        params_loader=PipesDatabricksNotebookWidgetsParamsLoader(dbutils.widgets),
    ) as pipes:
        value = pipes.get_extra("some_parameter")
        pipes.log.info(f"Running with some_parameter={value}")
        # ... your computation ...
        pipes.report_asset_materialization(
            metadata={"row_count": {"raw_value": 99_000, "type": "int"}},
        )

-------------------------------------------------------------------------------
"""

import os

import dagster as dg
from databricks.sdk import WorkspaceClient
from dagster_databricks import PipesDatabricksClient

# ---------------------------------------------------------------------------
# Demo flag
# ---------------------------------------------------------------------------

_DEMO_MODE: bool = (
    os.getenv("DEMO_MODE", "false").lower() == "true"
    or not os.getenv("DATABRICKS_CONNECTION_TOKEN")
)

# ---------------------------------------------------------------------------
# Resource
# ---------------------------------------------------------------------------

_pipes_resource = (
    dg.ResourceDefinition.none_resource()
    if _DEMO_MODE
    else PipesDatabricksClient(
        client=WorkspaceClient(
            host=os.environ["DATABRICKS_HOST"],
            token=os.environ["DATABRICKS_CONNECTION_TOKEN"],
        )
    )
)


@dg.definitions
def pipes_defs() -> dg.Definitions:
    return dg.Definitions(
        resources={"pipes_databricks": _pipes_resource},
    )
