-- Poll for new messasges and cleanup any expired jobs
WITH queue_info AS (
    SELECT
        id,
        ack_deadline,
        max_delivery_attempts
    FROM pgjobq.queues
    WHERE name = $1
), available_messsages AS (
    SELECT
        pgjobq.jobs.id,
        pgjobq.jobs.queue_id,
        pgjobq.jobs.delivery_attempts_remaining
    FROM pgjobq.jobs
    WHERE (
        available_at < now()::timestamp
        AND
        pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
        AND
        expires_at > now()::timestamp
        {where}
    )
    ORDER BY id -- to avoid deadlocks under concurrency
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgjobq.jobs
SET
    available_at = now() + (SELECT ack_deadline FROM queue_info),
    delivery_attempts_remaining = available_messsages.delivery_attempts_remaining - 1
FROM available_messsages
WHERE (
    pgjobq.jobs.queue_id = available_messsages.queue_id
    AND
    pgjobq.jobs.id = available_messsages.id
)
RETURNING
    pgjobq.jobs.id,
    available_at AS next_ack_deadline,
    body,
    pgjobq.jobs.attributes
;