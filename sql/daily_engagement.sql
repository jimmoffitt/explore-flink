-- 2) Analyze Daily Engagement Patterns for Each Movie Title:
SELECT
    SUBSTRING(datetime, 1, 10) AS browse_date,
    title,
    COUNT(*) AS daily_view_count,
    ROUND(AVG(duration),2) AS average_daily_watch_duration_seconds
FROM
    netflix_browse_avro
-- WHERE
--    title IS NOT NULL AND datetime IS NOT NULL AND duration IS NOT NULL
GROUP BY
    SUBSTRING(datetime, 1, 10), title
