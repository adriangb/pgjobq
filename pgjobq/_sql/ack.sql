WITH queue_info AS (
    SELECT
        name,
        id
    FROM pgjobq.queues
    WHERE name = $1
), jobs_for_deletion AS (
    SELECT
        id
    FROM pgjobq.jobs
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.jobs.id = $2
    )
    FOR UPDATE
), deleted_jobs AS (
    DELETE
    FROM pgjobq.jobs
    USING jobs_for_deletion
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.jobs.id = jobs_for_deletion.id
        AND
        receipt_handle = $3
    )
    RETURNING 1
)
-- this select returns NULL if no jobs were found, which means either
-- the owning queue was deleted or the job no longer belongs to this receiver
SELECT
    pg_notify('pgjobq.job_completed_' || $1, $2::text) AS notified,
    (SELECT EXISTS(SELECT * FROM queue_info)) AS queue_exists,
    (SELECT EXISTS(SELECT * FROM jobs_for_deletion)) AS job_exists,
    (SELECT NOT EXISTS(SELECT * FROM deleted_jobs)) AS receipt_handle_expired
;
