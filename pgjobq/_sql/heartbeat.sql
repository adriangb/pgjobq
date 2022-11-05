WITH queue_info AS (
    SELECT
        id,
        ack_deadline
    FROM pgjobq.queues
    WHERE name = $1
), job_info AS (
    SELECT
        unnest($2::uuid[]) AS id,
        unnest($3::bigint[]) AS receipt_handle
), selected_jobs AS (
    SELECT
        pgjobq.jobs.id
    FROM job_info
    JOIN pgjobq.jobs ON (
        pgjobq.jobs.id = job_info.id
        AND
        pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.jobs.receipt_handle = job_info.receipt_handle
    )
    WHERE (
        -- if the job expired we should keep ownership
        available_at > now()
    )
    ORDER BY id
    FOR UPDATE
)
UPDATE pgjobq.jobs
SET available_at = (
    GREATEST(
        (now() + (SELECT ack_deadline FROM queue_info)),
        available_at
    )
)
FROM selected_jobs
WHERE (
    pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
    AND
    pgjobq.jobs.id = selected_jobs.id
)
RETURNING available_at AS next_ack_deadline;