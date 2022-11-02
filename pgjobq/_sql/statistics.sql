WITH queue_info AS (
    SELECT
        id,
        max_size
    FROM pgjobq.queues
    WHERE name = $1
)
SELECT
    count(*) AS jobs,
    (SELECT max_size FROM queue_info) AS max_size
FROM pgjobq.jobs
WHERE queue_id = (SELECT id FROM queue_info);