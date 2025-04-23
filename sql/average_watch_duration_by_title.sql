-- 1) Calculate the Average Watch Duration for Each Movie Title Across All Users:
SELECT
    title,
    ROUND(AVG(duration),2) AS average_watch_duration_seconds
FROM netflix_browse_avro
GROUP BY
    title
