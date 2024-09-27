-- Prepare timeseries data
SELECT 
    time,
    tags,
    COUNT(*) as ""
FROM (
    SELECT 
        FROM_UNIXTIME(FLOOR(timestamp / 60) * 60) AS time, tags
        tags
    FROM 
        RedditComment.CommentResultTable
) AS inner_query
GROUP BY 
    time, tags
ORDER BY 
    time; 
