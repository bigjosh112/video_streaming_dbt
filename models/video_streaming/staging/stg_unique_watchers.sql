
WITH base_data AS (
    SELECT * 
    FROM {{ ref('dim_video_events') }}
)

SELECT
    month,
    COUNT(DISTINCT user_id) AS unique_watchers
FROM base_data
GROUP BY month