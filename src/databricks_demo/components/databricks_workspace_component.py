"""Custom Databricks Workspace Component.

Exposes a Databricks job as a Dagster asset using jobs.run_now().
No state management, no DBFS, no special token scopes beyond 'jobs'.

Configured via defs/databricks_workspace/defs.yaml.

Demo-safe: when DATABRICKS_HOST or DATABRICKS_CONNECTION_TOKEN are absent,
the asset materializes with placeholder data.
"""

import os
from dataclasses import dataclass

import dagster as dg
from dagster import Component, ComponentLoadContext, Resolvable


@dataclass
class DatabricksWorkspaceComponent(Component, Resolvable):
    """Triggers an existing Databricks job and surfaces it as a Dagster asset.

    Attributes:
        job_id:     Databricks job ID. Find it in Databricks UI:
                    Workflows > click your job > Job ID is in the URL.
        group_name: Dagster asset group name.
    """

    job_id: int
    group_name: str = "databricks_workspace"

    def build_defs(self, context: ComponentLoadContext) -> dg.Definitions:
        host = os.getenv("DATABRICKS_HOST")
        token = os.getenv("DATABRICKS_CONNECTION_TOKEN")
        _live = bool(host and token)

        job_id = self.job_id
        group_name = self.group_name

        @dg.asset(
            group_name=group_name,
            kinds={"databricks"},
            description=(
                "Triggers an existing Databricks job and waits for it to complete. "
                "Set DATABRICKS_HOST and DATABRICKS_CONNECTION_TOKEN to activate real execution."
            ),
        )
        def databricks_workspace_job(
            context: dg.AssetExecutionContext,
        ) -> dg.MaterializeResult:
            if not _live:
                context.log.info("DEMO_MODE: credentials not set — returning placeholder.")
                return dg.MaterializeResult(
                    metadata={
                        "job_id": dg.MetadataValue.text(str(job_id)),
                        "status": dg.MetadataValue.text("placeholder"),
                    }
                )

            from databricks.sdk import WorkspaceClient

            client = WorkspaceClient(host=host, token=token)

            context.log.info(f"Triggering Databricks job {job_id} on {host}")
            run = client.jobs.run_now(job_id=job_id).result()
            run_url = f"{host}/#job/{job_id}/run/{run.run_id}"

            context.log.info(f"Job completed — run_id={run.run_id}")
            return dg.MaterializeResult(
                metadata={
                    "job_id": dg.MetadataValue.text(str(job_id)),
                    "run_id": dg.MetadataValue.text(str(run.run_id)),
                    "run_url": dg.MetadataValue.url(run_url),
                    "status": dg.MetadataValue.text(str(run.state.result_state)),
                }
            )

        return dg.Definitions(assets=[databricks_workspace_job])
