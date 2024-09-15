-- models/staging/stg_total_watch_hours.sql
WITH base_data AS (
    SELECT * 
    FROM {{ ref('dim_video_events') }}
)

SELECT
    month,
    SUM(hour) AS total_watch_hours
FROM base_data
GROUP BY month