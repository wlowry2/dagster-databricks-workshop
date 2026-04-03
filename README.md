# Dagster + Databricks: Hands-On Workshop

A hands-on workshop where you work through three self-contained Dagster-Databricks integration patterns using your own Databricks account. Each pattern is independent — work through them in order or jump to the one most relevant to your use case.

---

## The Three Patterns

| # | Pattern | Best for |
|---|---------|----------|
| 1 | **Databricks Connect** | Centralized Python code that runs Spark remotely on Databricks compute |
| 2 | **Dagster Pipes** | Orchestrating notebooks with real-time log streaming and structured metadata |
| 3 | **Workspace Component** | Auto-discovering and orchestrating existing Databricks jobs — no Python required |

---

## Prerequisites

- Python 3.12+
- [`uv`](https://docs.astral.sh/uv/getting-started/installation/) installed
- A Databricks account with a workspace
- A Databricks personal access token

---

## Setup

```bash
# 1. Clone the repo
git clone <repo-url>
cd databricks

# 2. Install dependencies
uv sync

# 3. Configure your environment
cp .env.example .env
# Edit .env and fill in your values (see per-pattern instructions below)

# 4. Verify everything loads
uv run dg check defs
uv run dg list defs
```

> **No Databricks account?** Set `DEMO_MODE=true` in your `.env` to run all patterns with synthetic mock data.

---

## Pattern 1: Databricks Connect

**What it does:** Write Spark code in a Dagster asset. The Python logic runs in the Dagster process, but all Spark execution happens on a Databricks serverless cluster. Results (counts, metadata) stream back to Dagster.

**When to use it:** You want to keep pipeline logic centralized in Dagster and execute Spark workloads on demand — good for data validation, ad-hoc queries, and moderate Spark jobs.

**File:** `src/databricks_demo/defs/databricks_connect.py`
**Asset:** `spark_table_summary`

### Environment variables

```bash
DATABRICKS_HOST=...              # shared — already set
DATABRICKS_CONNECTION_TOKEN=...  # shared — already set
```

No extra variables needed beyond the shared credentials.

### Run it

```bash
uv run dagster asset materialize -m databricks_demo.definitions --select spark_table_summary
```

---

## Pattern 2: Dagster Pipes

**What it does:** Dagster submits a one-time notebook run to Databricks. The notebook uses the `dagster-pipes` library to stream logs and report structured asset metadata back to Dagster in real time.

**When to use it:** Your team already has Databricks notebooks doing meaningful work. You want Dagster to orchestrate them and get structured output (row counts, schema info, validation results) back without rewriting the notebook logic in Python.

**File:** `src/databricks_demo/defs/databricks_pipes.py`
**Asset:** `databricks_notebook_result`

### Databricks notebook setup

Your notebook needs to call back into Dagster via Pipes. Add this to the top of your notebook:

```python
from dagster_pipes import open_dagster_pipes

with open_dagster_pipes() as pipes:
    # your existing notebook code here
    
    # report results back to Dagster
    pipes.report_asset_materialization(
        metadata={
            "row_count": 99_000,
            "source_table": "workspace.workshop.customers_raw",
        }
    )
```

Install `dagster-pipes` on your cluster: `%pip install dagster-pipes`

### Environment variables

```bash
DATABRICKS_NOTEBOOK_PATH=/Users/you@company.com/your_notebook
DATABRICKS_CLUSTER_ID=          # optional — leave blank to auto-create serverless cluster
```

### Run it

```bash
uv run dagster asset materialize -m databricks_demo.definitions --select databricks_notebook_result
```

---

## Pattern 3: Workspace Component

**What it does:** Dagster connects to your Databricks workspace, discovers an existing job by its ID, and exposes it as a Dagster asset — automatically. No Python code required, just YAML configuration.

**When to use it:** You have existing Databricks jobs you want to orchestrate from Dagster without rewriting them. This is the fastest path to getting Databricks jobs into the Dagster asset graph.

**File:** `src/databricks_demo/defs/databricks_workspace/defs.yaml`

### Find your job ID

1. Open your Databricks workspace
2. Go to **Workflows** in the left sidebar
3. Click on any job
4. The job ID is in the URL: `.../jobs/218773222271247`

### Environment variables

```bash
DATABRICKS_JOB_ID=218773222271247   # replace with your job ID
```

### Run it

The discovered job appears automatically as an asset in the Dagster UI. Materializing it triggers a run of the Databricks job and waits for it to complete.

```bash
uv run dg list defs    # confirm the job asset appears
uv run dagster dev     # open the UI and materialize from there
```

---

## Example Pipelines

Two end-to-end pipelines show each pattern in a realistic context.

### Pipeline 1: Customer Data Validation (`customer_data_pipeline`)

**Scene:** Raw customer records land in Unity Catalog every hour. Before any report can run, Dagster validates row counts and null rates using Databricks Connect. Bad data stops the pipeline before it reaches downstream consumers.

```
raw_customer_table → validated_customer_data → customer_summary_report
                              ↑
                     Databricks Connect
                   (Spark SQL on serverless)
```

**File:** `src/databricks_demo/defs/data_validation_pipeline.py`

### Pipeline 2: Clickstream ML (`clickstream_ml_pipeline`)

**Scene:** A data engineering team has an existing Databricks notebook that transforms raw clickstream events into an ML feature table. Dagster triggers it via Pipes, streams logs back in real time, and only unblocks model training once the features are complete.

```
raw_clickstream_events → clickstream_feature_table → ml_model_training
                                   ↑
                            Dagster Pipes
                       (existing Databricks notebook)
```

**File:** `src/databricks_demo/defs/clickstream_ml_pipeline.py`

### Databricks setup

Before running the pipelines against a real Databricks account, create the required tables and upload the feature engineering notebook. See [`setup/README.md`](setup/README.md) for step-by-step instructions.

---

## Running with DEMO_MODE

All three patterns have a demo-safe mode that returns synthetic data without connecting to Databricks. Useful for exploring the project before setting up credentials.

```bash
DEMO_MODE=true uv run dagster asset materialize \
  -m databricks_demo.definitions \
  --select "spark_table_summary,databricks_notebook_result"
```

---

## Additional Resources

- [Databricks Connect docs](https://docs.dagster.io/integrations/libraries/databricks/databricks-connect)
- [Dagster Pipes docs](https://docs.dagster.io/integrations/libraries/databricks/dagster-databricks)
- [Workspace Component docs](https://docs.dagster.io/guides/labs/connections/databricks)
- [Dagster Slack community](https://dagster.io/slack)
