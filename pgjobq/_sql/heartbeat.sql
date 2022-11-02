WITH queue_info AS (
    SELECT
        id,
        ack_deadline
    FROM pgjobq.queues
    WHERE name = $1
), selected_jobs AS (
    SELECT
        id
    FROM pgjobq.jobs
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = any($2::uuid[])
        AND
        -- skip any jobs that already expired
        -- this avoids race conditions between
        -- extending deadlines and nacking
        available_at > now()
    )
    ORDER BY id
    FOR UPDATE
)
UPDATE pgjobq.jobs
SET available_at = (
    now() + (
        SELECT ack_deadline
        FROM queue_info
    )
)
FROM selected_jobs
WHERE (
    pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
    AND
    pgjobq.jobs.id = selected_jobs.id
)
RETURNING available_at AS next_ack_deadline;