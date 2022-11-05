WITH queue_info AS (
    SELECT
        id,
        name,
        max_delivery_attempts,
        retention_period
    FROM pgjobq.queues
    WHERE name = $1
), selected_jobs AS (
    SELECT
        id
    FROM pgjobq.jobs
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        {where}
    )
    ORDER BY id
    FOR UPDATE
), deleted_jobs AS (
    DELETE FROM pgjobq.jobs
    USING selected_jobs
    WHERE (
        pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.jobs.id = selected_jobs.id
    )
)
SELECT
    pg_notify('pgjobq.job_completed_' || $1, string_agg(id::text, ','))
FROM selected_jobs
;