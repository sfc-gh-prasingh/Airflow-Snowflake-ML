"""
Airflow DAG: Train and Register Demand Forecast Model

Trains a GradientBoostingRegressor on synthetic demand data and registers
it in the Snowflake Model Registry.

Adapted from Snowflake Task DAG version.
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"
MODEL_NAME = "DEMAND_FORECAST_MODEL"
VERSION = "V1"


@dag(
    dag_id="01_train_and_register",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "ml", "training"],
)
def train_and_register():

    @task()
    def train_and_register_model():
        import numpy as np
        import pandas as pd
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.model_selection import train_test_split
        from sklearn.metrics import mean_absolute_error, root_mean_squared_error

        from snowpark_session import create_snowpark_session
        from snowflake.ml.registry import Registry
        from snowflake.ml.model import type_hints as model_types

        np.random.seed(42)
        n = 5000
        data = pd.DataFrame({
            "day_of_week":     np.random.randint(0, 7, n).astype(float),
            "month":           np.random.randint(1, 13, n).astype(float),
            "avg_temperature": np.random.normal(60, 15, n),
            "promo_active":    np.random.choice([0.0, 1.0], n),
            "historical_avg":  np.random.normal(500, 100, n),
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

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        reg = Registry(session=session, database_name=DATABASE, schema_name=SCHEMA)
        sample_input = session.create_dataframe(X_test.head(10))

        reg.log_model(
            model=model,
            model_name=MODEL_NAME,
            version_name=VERSION,
            sample_input_data=sample_input,
            task=model_types.Task.TABULAR_REGRESSION,
            target_platforms=["WAREHOUSE"],
            metrics={"MAE": mae, "RMSE": rmse},
            comment="Demand forecast regression model for monitoring demo",
        )

        session.close()
        return {"model": MODEL_NAME, "version": VERSION, "mae": round(mae, 2), "rmse": round(rmse, 2)}

    train_and_register_model()


train_and_register()
