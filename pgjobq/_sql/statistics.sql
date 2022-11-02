WITH queue_info AS (
    SELECT
        id
    FROM pgjobq.queues
    WHERE name = $1
)
SELECT count(*) AS jobs
FROM pgjobq.jobs
WHERE queue_id = (SELECT id FROM queue_info);