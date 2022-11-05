WITH queue_info AS (
    SELECT
        id,
        ack_deadline,
        max_delivery_attempts,
        backoff_power_base
    FROM pgjobq.queues
    WHERE name = $1
), available_messsages AS (
    SELECT
        pgjobq.jobs.id,
        pgjobq.jobs.queue_id,
        delivery_attempts + 1 AS delivery_attempts
    FROM pgjobq.jobs
    WHERE (
        NOT EXISTS(
            SELECT * FROM pgjobq.predecessors WHERE pgjobq.predecessors.parent_id = pgjobq.jobs.id
        )
        AND
        pgjobq.jobs.available_at < now()::timestamp
        AND
        pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.jobs.expires_at > now()::timestamp
        {where}
    )
    ORDER BY id -- to avoid deadlocks under concurrency
    LIMIT $2
    FOR UPDATE SKIP LOCKED
), available_messages_with_queue_info AS (
    SELECT
        available_messsages.id,
        available_messsages.queue_id,
        pgjobq.calc_next_available_at(
            queue_info.ack_deadline,
            queue_info.backoff_power_base,
            available_messsages.delivery_attempts
        ) AS next_available_at,
        available_messsages.delivery_attempts AS delivery_attempts
    FROM available_messsages
    LEFT JOIN queue_info ON (
        available_messsages.queue_id = queue_info.id
    )
)
UPDATE pgjobq.jobs
SET
    available_at      = available_messages_with_queue_info.next_available_at,
    delivery_attempts = available_messages_with_queue_info.delivery_attempts,
    receipt_handle    = nextval('pgjobq.receipt_handles')
FROM available_messages_with_queue_info
WHERE (
    pgjobq.jobs.queue_id = available_messages_with_queue_info.queue_id
    AND
    pgjobq.jobs.id = available_messages_with_queue_info.id
)
RETURNING
    pgjobq.jobs.id,
    pgjobq.jobs.available_at AS next_ack_deadline,
    pgjobq.jobs.receipt_handle,
    pgjobq.jobs.body,
    pgjobq.jobs.attributes
;