
WITH base_data AS (
    SELECT * 
    FROM {{ ref('dim_video_events') }}
)

SELECT
    month,
    COUNT(DISTINCT user_id) AS multi_day_watchers
FROM (
    SELECT
        user_id,
        month,
        COUNT(DISTINCT day) AS unique_days_watched
    FROM base_data
    GROUP BY user_id, month
) AS daily_watchers
WHERE unique_days_watched > 1
GROUP BY month