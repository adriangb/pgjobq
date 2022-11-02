WITH queue_info AS (
    SELECT
        name,
        id
    FROM pgjobq.queues
    WHERE name = $1
)
DELETE
FROM pgjobq.jobs
WHERE (
    queue_id = (SELECT id FROM queue_info)
    AND
    pgjobq.jobs.id = $2
)
RETURNING
    pg_notify('pgjobq.job_completed_' || (SELECT name FROM queue_info), id::text) AS notified;
