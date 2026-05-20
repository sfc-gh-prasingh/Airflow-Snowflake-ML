"""
Airflow DAG: Continuous Drift Check & Retrain

Runs on a schedule to check the Model Monitor for feature drift (PSI > 0.2).
If drift is detected, retrains the model on updated data distribution and
registers a new version. Otherwise, skips.

Prerequisites: Run 04_monitoring_setup first to create the monitor and tables.
"""

from datetime import datetime

from airflow.decorators import dag, task

DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
WAREHOUSE = "MY_WAREHOUSE"
MODEL_NAME = "DEMAND_FORECAST_MODEL"
PSI_THRESHOLD = 0.2


@dag(
    dag_id="05_continuous_drift_retrain",
    schedule="0 */6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "ml", "monitoring", "continuous"],
)
def continuous_drift_retrain():

    @task()
    def check_drift():
        from snowpark_session import create_snowpark_session

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        result = session.sql(f"""
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

        max_psi = float(result[0]["MAX_PSI"]) if result[0]["MAX_PSI"] is not None else 0.0
        session.close()
        return {"max_psi": max_psi, "drift_detected": max_psi > PSI_THRESHOLD}

    @task.branch()
    def decide_retrain(drift_check: dict):
        if drift_check["drift_detected"]:
            return "retrain_model"
        return "skip_retrain"

    @task()
    def retrain_model():
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

        training_data = session.sql("""
            SELECT "day_of_week", "month", "avg_temperature", "promo_active",
                   "historical_avg", "DEMAND_ACTUAL"
            FROM DEMAND_PREDICTIONS
        """).to_pandas()

        X = training_data[["day_of_week", "month", "avg_temperature", "promo_active", "historical_avg"]]
        y = training_data["DEMAND_ACTUAL"]
        X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

        model = GradientBoostingRegressor(n_estimators=200, max_depth=4, random_state=42)
        model.fit(X_train, y_train)

        y_pred = model.predict(X_test)
        mae = mean_absolute_error(y_test, y_pred)
        rmse = root_mean_squared_error(y_test, y_pred)

        reg = Registry(session=session, database_name=DATABASE, schema_name=SCHEMA)

        existing_versions = reg.get_model(MODEL_NAME).show_versions()
        version_nums = [int(v["name"].replace("V", "")) for v in existing_versions if v["name"].startswith("V")]
        next_version = f"V{max(version_nums) + 1}"

        sample_input = session.create_dataframe(X_test.head(10).reset_index(drop=True))

        reg.log_model(
            model=model,
            model_name=MODEL_NAME,
            version_name=next_version,
            sample_input_data=sample_input,
            task=model_types.Task.TABULAR_REGRESSION,
            target_platforms=["WAREHOUSE"],
            metrics={"MAE": mae, "RMSE": rmse},
            comment=f"Auto-retrained due to drift (PSI > {PSI_THRESHOLD})",
        )

        session.close()
        return {"version": next_version, "mae": round(mae, 2), "rmse": round(rmse, 2)}

    @task()
    def skip_retrain():
        return {"action": "skipped", "reason": "No significant drift detected"}

    drift_check = check_drift()
    branch = decide_retrain(drift_check)
    retrain_model() >> branch
    skip_retrain() >> branch


continuous_drift_retrain()
