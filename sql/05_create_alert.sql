-- ============================================================
-- PART 5: Automated Alerting on Drift
-- Creates a Snowflake ALERT that checks PSI daily and fires
-- when drift exceeds a threshold.
-- ============================================================

USE DATABASE PRANJ;
USE SCHEMA TEST;
USE WAREHOUSE DEMO_WH;

-- -------------------------------------------------------
-- 5A: Create notification integration (one-time setup)
--     Replace email with your notification target
-- -------------------------------------------------------

CREATE NOTIFICATION INTEGRATION IF NOT EXISTS DRIFT_EMAIL_NOTIFICATION
    TYPE = EMAIL
    ENABLED = TRUE
    ALLOWED_RECIPIENTS = ('pranjal.singh@snowflake.com');

-- -------------------------------------------------------
-- 5B: Create the ALERT
--     Checks every hour; fires when PSI > 0.2
-- -------------------------------------------------------

CREATE OR REPLACE ALERT DEMAND_DRIFT_ALERT
    WAREHOUSE = DEMO_WH
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
            'pranjal.singh@snowflake.com',
            '[ALERT] Demand Model Drift Detected - Retraining Triggered',
            'The DEMAND_FORECAST_MODEL has detected significant feature drift (PSI > 0.2) in avg_temperature. The retrain pipeline has been triggered automatically.'
        );
        EXECUTE TASK PRANJ.TEST.RETRAIN_PIPELINE;
    END;

-- Enable the alert
ALTER ALERT DEMAND_DRIFT_ALERT RESUME;

-- Verify
SHOW ALERTS IN SCHEMA PRANJ.TEST;

-- -------------------------------------------------------
-- Cleanup commands (run after demo)
-- -------------------------------------------------------
-- ALTER ALERT DEMAND_DRIFT_ALERT SUSPEND;
-- DROP ALERT DEMAND_DRIFT_ALERT;
-- DROP MODEL MONITOR DEMAND_MONITOR;
-- DROP TABLE DEMAND_PREDICTIONS;
-- DROP TABLE DEMAND_BASELINE;
-- DROP MODEL DEMAND_FORECAST_MODEL;
