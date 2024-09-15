-- models/staging/stg_first_time_watchers.sql
WITH base_data AS (
    SELECT * 
    FROM {{ ref('dim_video_events') }}
)

SELECT
    month,
    COUNT(DISTINCT user_id) AS first_time_watchers
FROM base_data b1
WHERE NOT EXISTS (
    SELECT 1
    FROM base_data b2
    WHERE b2.user_id = b1.user_id
    AND b2.event_timestamp < b1.event_timestamp
)
GROUP BY month