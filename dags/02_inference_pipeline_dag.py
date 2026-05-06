"""
Airflow DAG: Monthly Inference Pipeline

Runs batch scoring using the registered model on the 1st of every month,
then logs the result to an inference log table.

Adapted from Snowflake Task DAG version.
"""

import sys
from datetime import datetime
from pathlib import Path

from airflow.decorators import dag, task

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

DATABASE = "MY_DATABASE"
SCHEMA = "MY_SCHEMA"


@dag(
    dag_id="02_inference_pipeline",
    schedule="0 6 1 * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["snowflake", "ml", "inference"],
)
def inference_pipeline():

    @task()
    def score_data():
        import json
        from snowpark_session import create_snowpark_session
        from snowflake.ml.registry import Registry

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        reg = Registry(session=session, database_name=DATABASE, schema_name=SCHEMA)
        mv = reg.get_model("DEMAND_FORECAST_MODEL").version("V1")

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.NEW_FEATURE_DATA AS
            SELECT
                UNIFORM(0, 6, RANDOM())::FLOAT AS "day_of_week",
                UNIFORM(1, 12, RANDOM())::FLOAT AS "month",
                NORMAL(60, 15, RANDOM())::FLOAT AS "avg_temperature",
                IFF(UNIFORM(0,1,RANDOM())>0.5, 1.0, 0.0) AS "promo_active",
                NORMAL(500, 100, RANDOM())::FLOAT AS "historical_avg"
            FROM TABLE(GENERATOR(ROWCOUNT => 500))
        """).collect()

        input_df = session.table(f"{DATABASE}.{SCHEMA}.NEW_FEATURE_DATA")
        predictions = mv.run(input_df, function_name="predict")
        predictions.write.save_as_table(f"{DATABASE}.{SCHEMA}.MONTHLY_PREDICTIONS", mode="overwrite")

        row_count = session.table(f"{DATABASE}.{SCHEMA}.MONTHLY_PREDICTIONS").count()
        session.close()
        return json.dumps({"status": "SUCCESS", "rows_scored": row_count})

    @task()
    def log_result(score_output: str):
        import json
        from snowpark_session import create_snowpark_session

        session = create_snowpark_session()
        session.sql(f"USE DATABASE {DATABASE}").collect()
        session.sql(f"USE SCHEMA {SCHEMA}").collect()

        session.sql(f"""
            CREATE TABLE IF NOT EXISTS {DATABASE}.{SCHEMA}.INFERENCE_LOG (
                RUN_DATE TIMESTAMP_NTZ,
                STATUS VARCHAR,
                ROWS_SCORED NUMBER
            )
        """).collect()

        info = json.loads(score_output)
        session.sql(f"""
            INSERT INTO {DATABASE}.{SCHEMA}.INFERENCE_LOG (RUN_DATE, STATUS, ROWS_SCORED)
            SELECT CURRENT_TIMESTAMP(), '{info["status"]}', {info["rows_scored"]}
        """).collect()

        session.close()
        return {"logged": True}

    score_output = score_data()
    log_result(score_output)


inference_pipeline()
