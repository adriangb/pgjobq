-- Poll for new messasges and cleanup any expired messages
WITH queue_info AS (
    SELECT
        id,
        ack_deadline,
        max_delivery_attempts
    FROM pgmq.queues
    WHERE name = $1
), available_messsages AS (
    SELECT
        pgmq.messages.id,
        pgmq.messages.queue_id,
        pgmq.messages.delivery_attempts_remaining
    FROM pgmq.messages
    WHERE (
        available_at < now()::timestamp
        AND
        pgmq.messages.queue_id = (SELECT id FROM queue_info)
        AND
        expires_at > now()::timestamp
    )
    ORDER BY id -- to avoid deadlocks under concurrency
    LIMIT $2
    FOR UPDATE SKIP LOCKED
)
UPDATE pgmq.messages
SET
    available_at = now() + (SELECT ack_deadline FROM queue_info),
    delivery_attempts_remaining = available_messsages.delivery_attempts_remaining - 1
FROM available_messsages
WHERE (
    pgmq.messages.queue_id = available_messsages.queue_id
    AND
    pgmq.messages.id = available_messsages.id
)
RETURNING
    pgmq.messages.id,
    available_at AS next_ack_deadline,
    body,
    pgmq.messages.attributes
;