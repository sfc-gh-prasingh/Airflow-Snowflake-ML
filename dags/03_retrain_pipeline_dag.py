"""
Airflow DAG: Drift-Triggered Retrain Pipeline

Checks model drift (PSI), branches on whether retraining is needed,
retrains the model if drift detected, and logs the result.

Adapted from Snowflake Task DAG version.
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task
from airflow.utils.trigger_rule import TriggerRule

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
WAREHOUSE = "MY_WAREHOUSE"


@dag(
    dag_id="03_retrain_pipeline",
    schedule="0 8 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "ml", "retrain", "drift"],
)
def retrain_pipeline():

    @task()
    def check_drift():
        import json
        from snowpark_session import create_snowpark_session

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        result = session.sql("""
            SELECT MAX(METRIC_VALUE) AS MAX_PSI
            FROM TABLE(MODEL_MONITOR_DRIFT_METRIC(
                'DEMAND_MONITOR',
                'POPULATION_STABILITY_INDEX',
                '"avg_temperature"',
                '1 DAY',
                DATEADD('day', -7, CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
                CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
            ))
        """).collect()

        max_psi = result[0]["MAX_PSI"] if result[0]["MAX_PSI"] is not None else 0.0
        session.close()
        return json.dumps({"max_psi": float(max_psi), "drift_detected": float(max_psi) > 0.2})

    @task.branch()
    def decide_retrain(drift_output: str):
        import json
        drift_info = json.loads(drift_output)
        if drift_info["drift_detected"]:
            return "retrain_model"
        return "skip_retrain"

    @task()
    def retrain_model():
        import json
        import numpy as np
        import pandas as pd
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_absolute_error, root_mean_squared_error

        from snowpark_session import create_snowpark_session
        from snowflake.ml.registry import Registry
        from snowflake.ml.model import type_hints as model_types

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        np.random.seed(99)
        n = 5000
        data = pd.DataFrame({
            "day_of_week":     np.random.randint(0, 7, n).astype(float),
            "month":           np.random.randint(1, 13, n).astype(float),
            "avg_temperature": np.random.normal(75, 15, n),
            "promo_active":    np.random.choice([0.0, 1.0], n),
            "historical_avg":  np.random.normal(425, 100, n),
        })
        data["demand"] = (
            200
            + 30 * data["promo_active"]
            + 0.8 * data["historical_avg"]
            + 1.2 * data["avg_temperature"]
            - 5 * data["day_of_week"]
            + np.random.normal(0, 20, n)
        )

        X = data.drop(columns=["demand"])
        y = data["demand"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = GradientBoostingRegressor(n_estimators=200, max_depth=4, random_state=42)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = root_mean_squared_error(y_test, y_pred)

        reg = Registry(session=session, database_name=DATABASE, schema_name=SCHEMA)
        sample_input = session.create_dataframe(X_test.head(10).reset_index(drop=True))

        reg.log_model(
            model=model,
            model_name="DEMAND_FORECAST_MODEL",
            version_name="V2",
            sample_input_data=sample_input,
            task=model_types.Task.TABULAR_REGRESSION,
            target_platforms=["WAREHOUSE"],
            metrics={"MAE": mae, "RMSE": rmse},
            comment="Retrained model on updated distribution (drift-triggered)",
        )

        session.close()
        return json.dumps({"version": "V2", "mae": round(mae, 2), "rmse": round(rmse, 2)})

    @task()
    def skip_retrain():
        import json
        return json.dumps({"action": "skipped", "reason": "No significant drift detected"})

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def notify_result(retrain_output=None, skip_output=None):
        import json
        from snowpark_session import create_snowpark_session

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.RETRAIN_LOG (
                RUN_DATE TIMESTAMP_NTZ,
                MESSAGE VARCHAR
            )
        """).collect()

        if retrain_output:
            info = json.loads(retrain_output)
            msg = f"Model retrained: V2 (MAE={info['mae']}, RMSE={info['rmse']})"
        else:
            msg = "Retraining skipped - no significant drift."

        session.sql(f"""
            INSERT INTO {DATABASE}.{SCHEMA}.RETRAIN_LOG (RUN_DATE, MESSAGE)
            SELECT CURRENT_TIMESTAMP(), '{msg}'
        """).collect()

        session.close()
        return {"logged": msg}

    drift_output = check_drift()
    branch_decision = decide_retrain(drift_output)
    retrain_out = retrain_model()
    skip_out = skip_retrain()
    notify_result(retrain_output=retrain_out, skip_output=skip_out)


retrain_pipeline()
