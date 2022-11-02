WITH queue_info AS (
    SELECT
        id,
        name,
        max_delivery_attempts,
        retention_period
    FROM pgmq.queues
    WHERE name = $1
), messages_to_delete AS (
    SELECT
        id,
        queue_id,
        body,
        (SELECT pg_notify('pgmq.message_completed_' || (SELECT name FROM queue_info), id::text)) AS notified
    FROM pgmq.messages
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        AND
        expires_at < now()::timestamp
        AND
        available_at < now()::timestamp
    )
    ORDER BY id -- to avoid deadlocks under concurrency
    LIMIT 100000 -- arbitrary, avoid hogging connections and locking
    FOR UPDATE SKIP LOCKED
), deleted_messages AS (
    DELETE FROM pgmq.messages
    USING messages_to_delete
    WHERE (
        pgmq.messages.queue_id = (SELECT id FROM queue_info)
        AND
        pgmq.messages.id = messages_to_delete.id
    )
), dlq_messages AS (
    SELECT
        d.id AS id,
        d.body AS body,
        dlq.queue_id,
        now()::timestamp + dlq.retention_period AS expires_at,
        dlq.max_delivery_attempts AS max_delivery_attempts
    FROM messages_to_delete d
    JOIN LATERAL (
        SELECT
            pgmq.queue_links.child_id AS queue_id,
            pgmq.queues.retention_period AS retention_period,
            pgmq.queues.max_delivery_attempts AS max_delivery_attempts
        FROM pgmq.queue_links
        LEFT JOIN pgmq.queues ON (
            link_type_id = (SELECT id FROM pgmq.queue_link_types WHERE name = 'dlq')
            AND
            pgmq.queue_links.child_id = pgmq.queues.id
        )
        WHERE pgmq.queue_links.parent_id = d.queue_id
    ) dlq ON true
)
INSERT INTO pgmq.messages(
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
FROM dlq_messages;