
WITH base_data AS (
    SELECT * 
    FROM {{ ref('dim_video_events') }}
)

SELECT
    b1.month,
    COUNT(DISTINCT b1.user_id) AS repeat_watchers
FROM base_data b1
JOIN base_data b2
ON b1.user_id = b2.user_id
AND b2.event_timestamp < b1.event_timestamp
GROUP BY b1.month