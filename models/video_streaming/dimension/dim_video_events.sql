WITH base_data AS (
    SELECT
        id,
        user_id,
        timestamp::timestamp AS event_timestamp,
        EXTRACT(MONTH FROM timestamp::timestamp) AS month,
        EXTRACT(HOUR FROM timestamp::timestamp) AS hour,
        EXTRACT(DAY FROM timestamp::timestamp) AS day,
        event
    FROM {{ source('video_streaming', 'video_events') }}
)
SELECT *
FROM base_data
WHERE NOT hour = 0 AND user_id IS NOT NULL
