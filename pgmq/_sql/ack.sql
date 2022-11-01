WITH queue_info AS (
    SELECT
        name
    FROM pgmq.queues
    WHERE name = $1
)
DELETE
FROM pgmq.messages
WHERE pgmq.messages.id = $2
RETURNING
    pg_notify('pgmq.message_completed_' || (SELECT name FROM queue_info), (SELECT id)::text) AS notified;
