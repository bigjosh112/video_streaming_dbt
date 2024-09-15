
SELECT
    uw.month,
    uw.unique_watchers,
    (tw.total_watch_hours / NULLIF(uw.unique_watchers, 0)) AS avg_watch_time,
    NULL AS percent_active_watched,
    ftw.first_time_watchers,
    (ftw.first_time_watchers::float / NULLIF(uw.unique_watchers, 0)) AS percent_unique_first_time,
    rw.repeat_watchers,
    mdw.multi_day_watchers
FROM {{ ref('stg_unique_watchers') }} uw
LEFT JOIN {{ ref('stg_total_watch_hours') }} tw ON uw.month = tw.month
LEFT JOIN {{ ref('stg_first_time_watchers') }} ftw ON uw.month = ftw.month
LEFT JOIN {{ ref('stg_repeat_watchers') }} rw ON uw.month = rw.month
LEFT JOIN {{ ref('stg_multi_day_watchers') }} mdw ON uw.month = mdw.month
ORDER BY uw.month