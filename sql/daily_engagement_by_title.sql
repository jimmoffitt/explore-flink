-- 3) Calculate Daily View Counts and Total Watch Time for Each Title:
SELECT
    SUBSTRING(datetime, 1, 10) AS browse_date,
    title,
    COUNT(*) AS daily_view_count,
    SUM(duration) AS total_daily_watch_time_seconds
FROM
    netflix_browse_avro
-- WHERE
--    title IS NOT NULL AND datetime IS NOT NULL AND duration IS NOT NULL
GROUP BY
    SUBSTRING(datetime, 1, 10), title
