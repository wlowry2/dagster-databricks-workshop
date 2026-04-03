# Workshop Setup

Run these steps once in your Databricks workspace before working through the pipelines.

## 1. Create the tables

Open a **SQL Worksheet** in Databricks (SQL Editor in the left sidebar) and run each script in order:

| Script | What it creates | Used by |
|--------|----------------|---------|
| `01_customers_raw.sql` | `main.default.customers_raw` — 1,500 fake customer rows | Pipeline 1 (Databricks Connect) |
| `02_clickstream_raw.sql` | `main.events.clickstream_raw` — 5,000 fake clickstream events | Pipeline 2 (Dagster Pipes) |

Each script ends with a `SELECT` that verifies the data was created correctly.

## 2. Upload the feature engineering notebook

1. In Databricks, go to **Workspace** in the left sidebar
2. Navigate to your user folder: `/Users/you@company.com/`
3. Click **Add** → **Import** → upload `03_feature_engineering_notebook.py`
4. Copy the full path (e.g. `/Users/you@company.com/feature_engineering`)
5. Add it to your `.env`:
   ```
   DATABRICKS_NOTEBOOK_PATH=/Users/you@company.com/feature_engineering
   ```

## 3. Check permissions

If running into permission errors, run this in a SQL Worksheet (ask your workspace admin if needed):

```sql
-- Replace `you@company.com` with your Databricks user or service principal
GRANT USE CATALOG ON CATALOG main              TO `you@company.com`;
GRANT USE SCHEMA  ON SCHEMA  main.default      TO `you@company.com`;
GRANT USE SCHEMA  ON SCHEMA  main.events       TO `you@company.com`;
GRANT USE SCHEMA  ON SCHEMA  main.features     TO `you@company.com`;
GRANT CREATE      ON SCHEMA  main.features     TO `you@company.com`;
GRANT SELECT      ON TABLE   main.default.customers_raw  TO `you@company.com`;
GRANT SELECT      ON TABLE   main.events.clickstream_raw TO `you@company.com`;
```

## 4. Run the pipelines

```bash
# With real Databricks (after filling in .env)
uv run dagster dev

# Without Databricks (demo mode)
DEMO_MODE=true uv run dagster dev
```

Then open `http://localhost:3000` and materialize either pipeline from the UI.
