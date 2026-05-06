"""
Airflow DAG: Monitoring Setup (One-Time)

Creates prediction/baseline tables, sets up the Model Monitor,
simulates drift, and creates an alert. Run once to bootstrap the
monitoring infrastructure.

Adapted from Snowflake Task DAG version.
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
WAREHOUSE = "MY_WAREHOUSE"
MODEL_NAME = "DEMAND_FORECAST_MODEL"
VERSION = "V1"


@dag(
    dag_id="04_monitoring_setup",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "ml", "monitoring", "setup"],
)
def monitoring_setup():

    @task()
    def create_prediction_tables():
        import numpy as np
        import pandas as pd
        from datetime import timedelta

        from snowpark_session import create_snowpark_session
        from snowflake.ml.registry import Registry

        np.random.seed(100)

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        reg = Registry(session=session, database_name=DATABASE, schema_name=SCHEMA)
        mv = reg.get_model(MODEL_NAME).version(VERSION)

        def generate_features(n, base_date, temp_mean=60, hist_mean=500):
            dates = [base_date + timedelta(hours=i) for i in range(n)]
            df = pd.DataFrame({
                "day_of_week":     [float(d.weekday()) for d in dates],
                "month":           [float(d.month) for d in dates],
                "avg_temperature": np.random.normal(temp_mean, 15, n),
                "promo_active":    np.random.choice([0.0, 1.0], n),
                "historical_avg":  np.random.normal(hist_mean, 100, n),
            })
            demand_true = (
                200
                + 30 * df["promo_active"]
                + 0.8 * df["historical_avg"]
                + 1.2 * df["avg_temperature"]
                - 5 * df["day_of_week"]
                + np.random.normal(0, 20, n)
            )
            return df, demand_true, dates

        baseline_features, baseline_demand, baseline_dates = generate_features(
            720, datetime(2026, 1, 1), temp_mean=60, hist_mean=500
        )
        baseline_sp = session.create_dataframe(baseline_features)
        baseline_preds = mv.run(baseline_sp, function_name="predict")
        baseline_pdf = baseline_preds.to_pandas()

        baseline_pdf["DEMAND_ACTUAL"] = baseline_demand.values
        baseline_pdf["TS"] = [pd.Timestamp(d).tz_localize(None) for d in baseline_dates]
        baseline_pdf["ROW_ID"] = range(len(baseline_pdf))
        baseline_pdf = baseline_pdf.rename(columns={"output_feature_0": "DEMAND_PREDICTION"})

        session.sql("DROP TABLE IF EXISTS DEMAND_BASELINE").collect()
        sp_baseline = session.create_dataframe(baseline_pdf)
        sp_baseline.write.save_as_table("DEMAND_BASELINE", mode="overwrite")

        pred_features, pred_demand, pred_dates = generate_features(
            336, datetime(2026, 2, 1), temp_mean=60, hist_mean=500
        )
        pred_sp = session.create_dataframe(pred_features)
        pred_preds = mv.run(pred_sp, function_name="predict")
        pred_pdf = pred_preds.to_pandas()

        pred_pdf["DEMAND_ACTUAL"] = pred_demand.values
        pred_pdf["TS"] = [pd.Timestamp(d).tz_localize(None) for d in pred_dates]
        pred_pdf["ROW_ID"] = range(len(pred_pdf))
        pred_pdf = pred_pdf.rename(columns={"output_feature_0": "DEMAND_PREDICTION"})

        session.sql("DROP TABLE IF EXISTS DEMAND_PREDICTIONS").collect()
        sp_pred = session.create_dataframe(pred_pdf)
        sp_pred.write.save_as_table("DEMAND_PREDICTIONS", mode="overwrite")

        session.close()
        return {"baseline_rows": len(baseline_pdf), "prediction_rows": len(pred_pdf)}

    @task()
    def create_monitor(table_info: dict):
        from snowpark_session import create_snowpark_session

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()
        session.sql(f"USE WAREHOUSE {WAREHOUSE}").collect()

        session.sql("DROP MODEL MONITOR IF EXISTS DEMAND_MONITOR").collect()

        session.sql(f"""
            CREATE MODEL MONITOR DEMAND_MONITOR WITH
                MODEL              = DEMAND_FORECAST_MODEL
                VERSION            = 'V1'
                FUNCTION           = 'predict'
                SOURCE             = {DATABASE}.{SCHEMA}.DEMAND_PREDICTIONS
                WAREHOUSE          = {WAREHOUSE}
                REFRESH_INTERVAL   = '1 day'
                AGGREGATION_WINDOW = '1 day'
                TIMESTAMP_COLUMN   = TS
                PREDICTION_SCORE_COLUMNS = ('DEMAND_PREDICTION')
                ACTUAL_SCORE_COLUMNS     = ('DEMAND_ACTUAL')
                BASELINE           = {DATABASE}.{SCHEMA}.DEMAND_BASELINE
                ID_COLUMNS         = ('ROW_ID')
        """).collect()

        session.close()
        return {"monitor": "DEMAND_MONITOR", "status": "created"}

    @task()
    def simulate_drift(monitor_info: dict):
        from snowpark_session import create_snowpark_session

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        session.sql("""
            INSERT INTO DEMAND_PREDICTIONS
                ("day_of_week", "month", "avg_temperature", "promo_active",
                 "historical_avg", "DEMAND_PREDICTION", "DEMAND_ACTUAL", "TS", "ROW_ID")
            SELECT
                UNIFORM(0, 6, RANDOM())::FLOAT,
                UNIFORM(1, 12, RANDOM())::FLOAT,
                NORMAL(90, 15, RANDOM())::FLOAT,
                IFF(UNIFORM(0, 1, RANDOM()) > 0.5, 1.0, 0.0),
                NORMAL(350, 100, RANDOM())::FLOAT,
                NORMAL(450, 80, RANDOM())::FLOAT,
                NORMAL(350, 60, RANDOM())::FLOAT,
                DATEADD('hour', SEQ4(), '2026-02-15'::TIMESTAMP_NTZ),
                336 + SEQ4()
            FROM TABLE(GENERATOR(ROWCOUNT => 336))
        """).collect()

        session.close()
        return {"drift_rows_inserted": 336}

    @task()
    def create_alert(drift_info: dict):
        from snowpark_session import create_snowpark_session

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        session.sql("""
            CREATE NOTIFICATION INTEGRATION IF NOT EXISTS DRIFT_EMAIL_NOTIFICATION
                TYPE = EMAIL
                ENABLED = TRUE
                ALLOWED_RECIPIENTS = ('your-email@example.com')
        """).collect()

        session.sql(f"""
            CREATE OR REPLACE ALERT DEMAND_DRIFT_ALERT
                WAREHOUSE = {WAREHOUSE}
                SCHEDULE  = '60 MINUTE'
            IF (EXISTS (
                SELECT METRIC_VALUE
                FROM TABLE(MODEL_MONITOR_DRIFT_METRIC(
                    'DEMAND_MONITOR',
                    'POPULATION_STABILITY_INDEX',
                    '"avg_temperature"',
                    '1 DAY',
                    DATEADD('day', -1, CURRENT_TIMESTAMP())::TIMESTAMP_NTZ,
                    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
                ))
                WHERE METRIC_VALUE > 0.2
            ))
            THEN
                BEGIN
                    CALL SYSTEM$SEND_EMAIL(
                        'DRIFT_EMAIL_NOTIFICATION',
                        'your-email@example.com',
                        '[ALERT] Demand Model Drift Detected',
                        'Significant feature drift (PSI > 0.2) detected in avg_temperature.'
                    );
                END
        """).collect()

        session.sql("ALTER ALERT DEMAND_DRIFT_ALERT RESUME").collect()
        session.close()
        return {"alert": "DEMAND_DRIFT_ALERT", "status": "active"}

    tables = create_prediction_tables()
    monitor = create_monitor(tables)
    drift = simulate_drift(monitor)
    create_alert(drift)


monitoring_setup()
