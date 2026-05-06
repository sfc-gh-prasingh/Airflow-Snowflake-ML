# Snowflake ML Airflow Example

ML pipeline orchestration using Apache Airflow with Snowflake ML — demonstrating model training, batch inference, drift-triggered retraining, and monitoring.

## DAGs

| DAG | Schedule | Description |
|-----|----------|-------------|
| `01_train_and_register` | Manual | Train GBR model + register in Snowflake Model Registry |
| `02_inference_pipeline` | Monthly (1st, 6am PT) | Batch scoring with registered model + logging |
| `03_retrain_pipeline` | Monthly (1st, 8am PT) | Drift check → branch → retrain or skip → log |
| `04_monitoring_setup` | Manual (one-time) | Create prediction tables, monitor, simulate drift, alert |

## Prerequisites

- Python 3.11+
- Snowflake account with Model Registry, Model Monitor support
- Snowflake CLI connection configured in `~/.snowflake/connections.toml`
- Model `PRANJ.TEST.DEMAND_FORECAST_MODEL` registered (run DAG 01 first)

## Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Initialize Airflow (if not already done)
export AIRFLOW_HOME=~/airflow
airflow db init

# Copy DAGs to Airflow dags folder (or symlink)
ln -s $(pwd)/dags/* $AIRFLOW_HOME/dags/

# Ensure snowpark_session.py is importable
export PYTHONPATH=$(pwd):$PYTHONPATH

# Start Airflow
airflow standalone
```

## Connection Configuration

The DAGs use `snowpark_session.py` which reads from `~/.snowflake/connections.toml`. Set the connection name via environment variable:

```bash
export SNOWFLAKE_CONNECTION_NAME=demoacct
```

## Execution Order

1. **01_train_and_register** — Train and register the base model (V1)
2. **04_monitoring_setup** — Create tables, monitor, simulate drift, alert
3. **02_inference_pipeline** — Runs monthly for batch scoring
4. **03_retrain_pipeline** — Runs monthly; retrains if drift detected (PSI > 0.2)

## Key Differences from Snowflake Task DAG Version

| Concept | Snowflake Tasks | Airflow |
|---------|----------------|---------|
| DAG definition | `snowflake.core.task.dagv1.DAG` | `@dag` decorator |
| Task | `DAGTask` | `@task` decorator |
| Branching | `DAGTaskBranch` | `@task.branch` |
| Data passing | `SYSTEM$GET_PREDECESSOR_RETURN_VALUE` | XCom (automatic in TaskFlow) |
| Deployment | `DAGOperation.deploy()` | Auto-discovered from `dags_folder` |
| Scheduling | `Cron("0 8 1 * *", "America/Los_Angeles")` | `schedule="0 8 1 * *"` |
