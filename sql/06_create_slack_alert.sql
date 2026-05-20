-- ============================================================
-- PART 6: Slack Notification Alert for Drift
-- Creates a webhook-based notification integration for Slack
-- and a separate alert that posts to a Slack channel.
-- ============================================================

USE DATABASE PRANJ;
USE SCHEMA TEST;
USE WAREHOUSE DEMO_WH;

-- -------------------------------------------------------
-- 6A: Create a secret for your Slack webhook URL
--     Replace with your actual Slack incoming webhook secret
--     (the part after https://hooks.slack.com/services/)
-- -------------------------------------------------------

CREATE OR REPLACE SECRET SLACK_WEBHOOK_SECRET
    TYPE = GENERIC_STRING
    SECRET_STRING = 'T00000000/B00000000/XXXXXXXXXXXXXXXXXXXXXXXX';

-- -------------------------------------------------------
-- 6B: Create the webhook notification integration for Slack
-- -------------------------------------------------------

CREATE OR REPLACE NOTIFICATION INTEGRATION DRIFT_SLACK_NOTIFICATION
    TYPE = WEBHOOK
    ENABLED = TRUE
    WEBHOOK_URL = 'https://hooks.slack.com/services/SNOWFLAKE_WEBHOOK_SECRET'
    WEBHOOK_SECRET = PRANJ.TEST.SLACK_WEBHOOK_SECRET
    WEBHOOK_BODY_TEMPLATE = '{"text": "SNOWFLAKE_WEBHOOK_MESSAGE"}'
    WEBHOOK_HEADERS = ('Content-Type'='application/json');

-- -------------------------------------------------------
-- 6C: Create a Slack-specific drift alert
--     Checks every hour; posts to Slack when PSI > 0.2
-- -------------------------------------------------------

CREATE OR REPLACE ALERT DEMAND_DRIFT_SLACK_ALERT
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
        CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
            SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
                SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT(
                    ':rotating_light: *ML Drift Alert* - DEMAND_FORECAST_MODEL has significant feature drift (PSI > 0.2) in avg_temperature. Retrain pipeline triggered.'
                )
            ),
            SNOWFLAKE.NOTIFICATION.INTEGRATION('DRIFT_SLACK_NOTIFICATION')
        );
        EXECUTE TASK PRANJ.TEST.RETRAIN_PIPELINE;
    END;

-- Enable the alert
ALTER ALERT DEMAND_DRIFT_SLACK_ALERT RESUME;

-- Verify
SHOW ALERTS IN SCHEMA PRANJ.TEST;

-- -------------------------------------------------------
-- Test: Manually trigger the Slack notification
-- -------------------------------------------------------
-- CALL SYSTEM$SEND_SNOWFLAKE_NOTIFICATION(
--     SNOWFLAKE.NOTIFICATION.TEXT_PLAIN(
--         SNOWFLAKE.NOTIFICATION.SANITIZE_WEBHOOK_CONTENT(
--             ':test_tube: Test notification from Snowflake ML Monitor'
--         )
--     ),
--     SNOWFLAKE.NOTIFICATION.INTEGRATION('DRIFT_SLACK_NOTIFICATION')
-- );

-- -------------------------------------------------------
-- Cleanup
-- -------------------------------------------------------
-- ALTER ALERT DEMAND_DRIFT_SLACK_ALERT SUSPEND;
-- DROP ALERT DEMAND_DRIFT_SLACK_ALERT;
-- DROP NOTIFICATION INTEGRATION DRIFT_SLACK_NOTIFICATION;
-- DROP SECRET SLACK_WEBHOOK_SECRET;
