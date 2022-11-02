WITH queue_info AS (
    SELECT
        id,
        name,
        max_delivery_attempts,
        retention_period
    FROM pgjobq.queues
    WHERE name = $1
), updated_jobs AS (
    UPDATE pgjobq.jobs
    -- make it available in the past to avoid race conditions with extending deadlines
    -- which check to make sure the job is still available before extending
    SET
        available_at = now() - '1 second'::interval
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = $2
        AND
        delivery_attempts_remaining != 0
    )
    RETURNING
        (SELECT pg_notify('pgjobq.new_job_' || (SELECT name FROM queue_info), '')) AS notified
), deleted_jobs AS (
    DELETE FROM pgjobq.jobs
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        id = $2
        AND
        delivery_attempts_remaining = 0
    )
    RETURNING
        id,
        queue_id,
        body,
        (SELECT pg_notify('pgjobq.job_completed_' || (SELECT name FROM queue_info), id::text)) AS notified
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
)
INSERT INTO pgjobq.jobs(
    queue_id,
    id,
    expires_at,
    delivery_attempts_remaining,
    available_at,
    body
)
SELECT
    queue_id,
    id,
    expires_at,
    max_delivery_attempts,
    now()::timestamp,
    body
FROM dlq_jobs
RETURNING
    (SELECT notified FROM deleted_jobs) AS completed_notification,
    (SELECT notified FROM updated_jobs) AS new_job_notification
;