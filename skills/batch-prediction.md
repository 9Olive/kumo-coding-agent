# Run Batch Predictions (fine-tuned SDK)

Generate predictions from a trained model on new data — either in a one-off
session or as part of a scheduled DAG — using the Kumo fine-tuned SDK.

---

## Prerequisites

- Completed training job (follow `skills/train-model.md`)
- Training job ID saved to `scratch/` (e.g., `trainingjob-abc123...`) or passed as context otherwise.
- `kumoai` installed (`uv add kumoai` — see `context/platform/data-connectors.md` for full setup)
- API credentials set: `KUMO_API_URL`, `KUMO_API_KEY` (or pass to `kumoai.init()`)
- **Read first**: `context/platform/sdk-overview.md`

---

## Workflow

### Step 1: Initialize and Load from Training Job

Load a `Trainer` and `PredictiveQuery` from a saved training job ID. This
reconnects to a completed training job — from a new session, a different
machine, or a scheduled script — without rerunning training.

```python
import kumoai

kumoai.init(url="https://app.kumo.ai", api_key="YOUR_API_KEY")

trainer = kumoai.Trainer.load("trainingjob-abc123...")
pquery  = kumoai.PredictiveQuery.load_from_training_job("trainingjob-abc123...")
# pquery.graph now holds the original training graph
```

**Expected output**: Both calls return without error. `pquery.graph` is
populated with the connector and table definitions used during training.

Always save training job IDs to `scratch/YYYY-MM-DD_<task>.md` during
training — they're the only handle you need to resume predictions in any
future session.

### Step 2: Generate Prediction Table

WARNING - **Static (non-temporal) predictions**: If the target table in the graph has no time column, the task is treated as imputation — only rows where the target column is `NULL`
in the source table are scored. Before running, confirm that new entities or
rows needing predictions have a `NULL` target value in the source data. Review design patterns in section 5b for swapping tables / updating graph at batch prediction. 

Build the prediction table that defines *who* gets scored and at *what time*.

```python
import datetime

pred_plan = pquery.suggest_prediction_table_plan()

# Set the anchor time — the point-in-time cutoff for features
pred_plan.anchor_time = datetime.datetime(2025, 6, 1)   # historical date
# or: pred_plan.anchor_time = datetime.datetime.now()   # score as of today

# For forecasting tasks only:
# pred_plan.forecast_length = 12 # <-- This is an arbitrary value. Values should match training job configuration. 
# pred_plan.lag_timesteps = 7 # <-- This is an arbitrary value. Values should match training job co
nfiguration.

pred_table_job = pquery.generate_prediction_table(pred_plan, non_blocking=True)
pred_table = pred_table_job.attach()
```

### Step 3: Configure Output — Write-New vs Append

Choose an output strategy based on your DAG design and downstream consumption
pattern.

**Option A — Write a new table per run** (date-partitioned; idempotent DAGs):

Each run lands in a fresh destination. Downstream consumers read the latest
partition directly.

```python
from datetime import date

output_table = f"revenue_predictions_{date.today().strftime('%Y%m%d')}"

output_config = kumoai.OutputConfig(
    output_types={"predictions"},      # or {"predictions", "embeddings"}
    output_connector=connector,
    output_table_name=output_table,
)
```

**Option B — Append to a single table** (stable name; metadata-partitioned):

All runs land in one table. Use metadata fields so downstream consumers can
filter or partition by run time.

```python
output_config = kumoai.OutputConfig(
    output_types={"predictions"},
    output_connector=connector,
    output_table_name="revenue_predictions",
    output_metadata_fields=[
        kumoai.MetadataField.JOB_TIMESTAMP,
        kumoai.MetadataField.ANCHOR_TIMESTAMP,
    ],
)
```

`JOB_TIMESTAMP` and `ANCHOR_TIMESTAMP` are appended as columns to every output
row. Use them as partition or filter keys downstream to distinguish runs.

**Choosing between options:**

| | Write-New | Append |
|---|---|---|
| Idempotent re-runs | Safe — each run is isolated | Requires deduplication logic |
| Storage growth | Unbounded (one table per run) | Compact (one table total) |
| Downstream query | Read latest partition | Filter on `JOB_TIMESTAMP` |
| Best for | Batch ETL, S3 pipelines | Snowflake analytics, dashboards |

### Step 4: Run the Prediction Job

```python
prediction_job = trainer.predict(
    graph=pquery.graph,
    prediction_table=pred_table,
    output_config=output_config,
    num_workers=8,
    non_blocking=True,
)

print(f"Prediction job ID: {prediction_job.job_id}")
print(f"Status: {prediction_job.status()}")   # 'PENDING', 'RUNNING', 'COMPLETED', 'FAILED'

# Block until complete
prediction_result = prediction_job.attach()
```

**Save the prediction job ID** — like training jobs, predictions are
long-running. Persist the ID so you can re-attach from any session:

```python
print(f"Prediction job ID: {prediction_job.job_id}")
# Write this to scratch/YYYY-MM-DD_<task>.md

# Re-attach in a later session
prediction_job = kumoai.PredictionJob(job_id="predictionjob-xyz...")
prediction_result = prediction_job.attach()
```


### Step 5: Design Patterns

#### 5a: Graph Swap for Link Prediction (LHS / RHS Cadence Split)

Link prediction models learn joint embeddings for both sides of the
relationship — Left Hand Side (LHS, e.g. users) and Right Hand Side (RHS,
e.g. ads or items). LHS and RHS often need to be scored at different cadences:
RHS entities may change daily (new ads, new products) while LHS entities turn
over more slowly (weekly user re-scoring).

The pattern: rebuild the graph using the **same table name aliases** as the
training graph, but point each table to a source that contains only the
entities relevant to this run. Because the alias matches, the trained model
sees the same schema it was trained on; only the rows differ.

```python
# Example: script accepts --side {ad,user} as a CLI argument.
# The graph is rebuilt with the same aliases but different source tables.

trainer = kumoai.Trainer.load("trainingjob-abc123...")
pquery  = kumoai.PredictiveQuery.load_from_training_job("trainingjob-abc123...")

graph = kumoai.Graph(name="prediction_graph", connector=connector)
graph.add_table(kumoai.Table(
    name="ads",                              # same alias as training graph
    data=connector.table(args.ads_source),   # source chosen by CLI arg
    ...
))
# ... add remaining tables and edges with the same aliases as training ...
pquery.graph = graph
```

The workflow then invokes the script twice with different arguments — once for
each side — rather than the agent or script looping internally:

```yaml
jobs:
  rhs_embeddings:
    steps:
      - run: python model_run.py --side ad --predictiontime "$(date +'%Y-%m-%d')"
  lhs_embeddings:
    steps:
      - run: python model_run.py --side user --predictiontime "$(date +'%Y-%m-%d')"
```

**Constraint**: table name aliases, column names, dtypes, and edge structure
must exactly match the training-time graph. Only the underlying rows can differ.

#### 5b: Table Swap for Static Predictions (Imputation)

Static (non-temporal) models score entities with a `NULL` target. To score
newly arrived entities without retraining, update the source table in your
connector to include new rows with `NULL` targets, then re-generate the
prediction table against the existing graph.

```python
# 1. Add new entities to the source table in your data warehouse
#    (e.g., INSERT new rows with target = NULL into Snowflake/BQ/Databricks/SQL or rewrite static files)

# 2. Re-generate the prediction table — it will now include the new rows
pred_plan = pquery.suggest_prediction_table_plan()
pred_table = pquery.generate_prediction_table(pred_plan)

# 3. Score with the same trained model
prediction_job = trainer.predict(
    graph=pquery.graph,
    prediction_table=pred_table,
    output_config=output_config,
)
```

No graph rebuild or model retraining is needed — the model scores only the
new `NULL`-target rows.

### Step 6: DAG Integration (GitHub Actions Patterns)

Three patterns distilled from production deployments:

#### Pattern A — Simple daily prediction

One job, one script, cron-scheduled. Appropriate for most use cases.

```yaml
on:
  schedule:
    - cron: "0 6 * * *"   # 6 AM UTC daily
  workflow_dispatch:       # allow manual trigger

jobs:
  run_prediction:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.10" }
      - run: pip install kumoai
      - run: |
          python batch_prediction.py \
            --trainingjobid trainingjob-abc123... \
            --predictiontime "$(date +'%Y-%m-%d')" \
            --numworkers 8
        env:
          KUMO_API_KEY: ${{ secrets.KUMO_API_KEY }}
          KUMO_API_URL: ${{ secrets.KUMO_API_URL }}
```

#### Pattern B — Async submit + status poller

Use when predictions take longer than a GitHub Actions job timeout, or when
you want decoupled alerting. One workflow submits the job (`non_blocking=True`)
and saves the prediction job ID; a separate poller runs every 30 minutes to
check status and alert on failure.

```yaml
# poll_job_status.yml
on:
  schedule:
    - cron: "0,30 * * * *"   # every 30 minutes
  workflow_dispatch:

jobs:
  poll:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-python@v5
        with: { python-version: "3.11" }
      - run: pip install kumoai
      - run: python monitoring/job_polling.py
        env:
          KUMO_API_KEY: ${{ secrets.KUMO_API_KEY }}
          WEBHOOK_URL:  ${{ secrets.WEBHOOK_URL }}

  alert_on_failure:
    needs: [poll]
    if: ${{ always() && contains(needs.*.result, 'failure') }}
    runs-on: ubuntu-latest
    steps:
      - run: |
          curl -X POST "${{ secrets.INCIDENT_WEBHOOK }}" \
            -H "Content-Type: application/json" \
            -d '{"title": "Kumo prediction job failed", "urgency": "high"}'
```

#### Pattern C — Embedding pair (link prediction)

Run both sides of a link graph on the same schedule. Jobs can run in parallel
when there is no data dependency between them.

```yaml
jobs:
  ad_embeddings:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install kumoai
      - run: |
          python model_run.py \
            --embeddingmode ad \
            --predictiontime "$(date +'%Y-%m-%d')"
        env:
          KUMO_API_KEY: ${{ secrets.KUMO_API_KEY }}

  user_embeddings:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: pip install kumoai
      - run: |
          python model_run.py \
            --embeddingmode user \
            --predictiontime "$(date +'%Y-%m-%d')"
        env:
          KUMO_API_KEY: ${{ secrets.KUMO_API_KEY }}
```

---

## Quick Reference

| Step | Method | Key Arguments |
|------|--------|---------------|
| Load trainer | `kumoai.Trainer.load(job_id)` | training job ID string |
| Load pquery | `kumoai.PredictiveQuery.load_from_training_job(job_id)` | training job ID string |
| Plan prediction table | `pquery.suggest_prediction_table_plan()` | `run_mode` |
| Set anchor time | `pred_plan.anchor_time = datetime.datetime(...)` | datetime object |
| Generate prediction table | `pquery.generate_prediction_table(plan, non_blocking=True)` | `non_blocking` |
| Swap graph | `pquery.graph = new_graph` | replacement graph object |
| Configure output (new table) | `kumoai.OutputConfig(output_table_name=f"table_{date}")` | date-stamped name |
| Configure output (append) | `kumoai.OutputConfig(..., output_metadata_fields=[...])` | `JOB_TIMESTAMP`, `ANCHOR_TIMESTAMP` |
| Run prediction | `trainer.predict(graph, pred_table, output_config, ...)` | `num_workers`, `non_blocking` |
| Check job status | `prediction_job.status()` | — |
| Reattach prediction | `kumoai.PredictionJob(job_id).attach()` | prediction job ID |
| Get results as DataFrame | `prediction_result.predictions_df()` | — |

---

## Common Errors

| Error | Cause | Fix |
|-------|-------|-----|
| `ResourceNotFoundError` on `Trainer.load()` | Training job ID is wrong or job was deleted | Verify ID in scratch file or Kumo dashboard |
| `ResourceNotFoundError` on `PredictiveQuery.load_from_training_job()` | Same as above | Same fix |
| Prediction table is empty | Static task with no `NULL` targets in source | Ensure new entities have `NULL` target values before generating prediction table |
| Graph validation error after swap | Swapped graph is missing tables or edges the PQL query references | Swapped graph must cover all tables and edges in the original PQL query |
| Output table already exists | Write-new strategy with a name collision | Include date/timestamp in `output_table_name` |
| `JobFailedError` during prediction | Graph schema mismatch vs training time | Column names and dtypes must exactly match training-time schema |
| Job stalls in `RUNNING` state | Job queued or stalled | Call `prediction_job.status()` for the event log; check `prediction_job.error_message()` |
| No embeddings in output | `output_types={"predictions"}` used for a link prediction model | Set `output_types={"embeddings"}` (or `{"predictions", "embeddings"}`) |
| `ConnectionError` | Bad credentials or unreachable API | Verify `kumoai.init()` parameters and network access |

---

## Checklist

- [ ] Training job ID retrieved from scratch and verified (`Trainer.load()` succeeds)
- [ ] `PredictiveQuery` loaded from training job — `pquery.graph` is populated
- [ ] Anchor time set to the correct prediction window cutoff
- [ ] For static tasks: source table updated so new entities have `NULL` target values
- [ ] For link prediction: side-specific graph built and swapped onto `pquery`
- [ ] Output strategy chosen: write-new (date-stamped name) or append (with `MetadataField`)
- [ ] Prediction table row count verified before submitting job
- [ ] Prediction job submitted — job ID saved to scratch
- [ ] `prediction_result.predictions_df()` returns expected rows and columns
- [ ] DAG workflow configured if predictions should run on a recurring schedule
