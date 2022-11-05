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
        id,
        receipt_handle
    FROM pgjobq.jobs
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = $2
    )
    ORDER BY id
    FOR UPDATE
), jobs_to_process AS (
    SELECT id
    FROM selected_jobs
    WHERE receipt_handle = $3
), deleted_jobs AS (
    DELETE FROM pgjobq.jobs
    USING jobs_to_process
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.jobs.id = jobs_to_process.id
        AND
        delivery_attempts >= (SELECT max_delivery_attempts FROM queue_info)
    )
    RETURNING
        pgjobq.jobs.id,
        pgjobq.jobs.queue_id,
        pgjobq.jobs.body
), dlq_jobs AS (
    SELECT
        d.id AS id,
        d.body AS body,
        dlq.queue_id,
        now()::timestamp + dlq.retention_period AS expires_at,
        dlq.max_delivery_attempts AS max_delivery_attempts
    FROM deleted_jobs d
    JOIN LATERAL (
        SELECT
            pgjobq.queue_links.child_id AS queue_id,
            pgjobq.queues.retention_period AS retention_period,
            pgjobq.queues.max_delivery_attempts AS max_delivery_attempts
        FROM pgjobq.queue_links
        LEFT JOIN pgjobq.queues ON (
            parent_id = d.queue_id
            AND
            link_type_id = (SELECT id FROM pgjobq.queue_link_types WHERE name = 'dlq')
            AND
            pgjobq.queue_links.child_id = pgjobq.queues.id
        )
    ) dlq ON true
), new_dlq_jobs AS (
    INSERT INTO pgjobq.jobs(
        queue_id,
        id,
        expires_at,
        delivery_attempts,
        available_at,
        body
    )
    SELECT
        queue_id,
        id,
        expires_at,
        0,
        now()::timestamp,
        body
    FROM dlq_jobs
)
-- this select returns NULL if no jobs were found, which means either
-- the owning queue was deleted or the job no longer belongs to this receiver
SELECT
    (SELECT pg_notify('pgjobq.job_completed_' || (SELECT name FROM queue_info), string_agg(id::text, ',')) FROM deleted_jobs) AS deleted_notify,
    (SELECT pg_notify('pgjobq.new_job_' || (SELECT name FROM queue_info), (SELECT count(*)::text FROM deleted_jobs))) AS new_dlq_jobs_notify,
    (SELECT EXISTS(SELECT * FROM queue_info)) AS queue_exists,
    (SELECT EXISTS(SELECT * FROM selected_jobs)) AS job_exists,
    (SELECT NOT EXISTS(SELECT * FROM jobs_to_process)) AS receipt_handle_expired
FROM jobs_to_process;