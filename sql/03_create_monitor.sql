-- ============================================================
-- PART 3: Create Model Monitor with Drift Detection
-- Run in Snowflake (Snowsight, snowsql, or Cortex Code)
-- ============================================================

USE DATABASE PRANJ;
USE SCHEMA TEST;
USE WAREHOUSE DEMO_WH;

-- Drop existing monitor if re-running
DROP MODEL MONITOR IF EXISTS DEMAND_MONITOR;

-- Create the model monitor
CREATE MODEL MONITOR DEMAND_MONITOR WITH
    MODEL              = DEMAND_FORECAST_MODEL
    VERSION            = 'V1'
    FUNCTION           = 'predict'
    SOURCE             = PRANJ.TEST.DEMAND_PREDICTIONS
    WAREHOUSE          = DEMO_WH
    REFRESH_INTERVAL   = '1 day'
    AGGREGATION_WINDOW = '1 day'
    TIMESTAMP_COLUMN   = TS
    PREDICTION_SCORE_COLUMNS = ('DEMAND_PREDICTION')
    ACTUAL_SCORE_COLUMNS     = ('DEMAND_ACTUAL')
    BASELINE           = PRANJ.TEST.DEMAND_BASELINE
    ID_COLUMNS         = ('ROW_ID');

-- Verify the monitor is active
DESC MODEL MONITOR DEMAND_MONITOR;

-- List all monitors
SHOW MODEL MONITORS IN SCHEMA PRANJ.TEST;
