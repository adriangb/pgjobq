WITH queue_info AS (
    SELECT
        id,
        name,
        max_delivery_attempts,
        retention_period
    FROM pgmq.queues
    WHERE name = $1
), updated_messages AS (
    UPDATE pgmq.messages
    -- make it available in the past to avoid race conditions with extending deadlines
    -- which check to make sure the message is still available before extending
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
        (SELECT pg_notify('pgmq.new_message_' || (SELECT name FROM queue_info), '')) AS notified
), deleted_messages AS (
    DELETE FROM pgmq.messages
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
        (SELECT pg_notify('pgmq.message_completed_' || (SELECT name FROM queue_info), id::text)) AS notified
), dlq_messages AS (
    SELECT
        d.id AS id,
        d.body AS body,
        dlq.queue_id,
        now()::timestamp + dlq.retention_period AS expires_at,
        dlq.max_delivery_attempts AS max_delivery_attempts
    FROM deleted_messages d
    JOIN LATERAL (
        SELECT
            pgmq.queue_links.child_id AS queue_id,
            pgmq.queues.retention_period AS retention_period,
            pgmq.queues.max_delivery_attempts AS max_delivery_attempts
        FROM pgmq.queue_links
        LEFT JOIN pgmq.queues ON (
            parent_id = d.queue_id
            AND
            link_type_id = (SELECT id FROM pgmq.queue_link_types WHERE name = 'dlq')
            AND
            pgmq.queue_links.child_id = pgmq.queues.id
        )
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
FROM dlq_messages
RETURNING
    (SELECT notified FROM deleted_messages) AS completed_notification,
    (SELECT notified FROM updated_messages) AS new_message_notification
;