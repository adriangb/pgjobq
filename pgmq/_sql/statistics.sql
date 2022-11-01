WITH queue_info AS (
    SELECT
        id
    FROM pgmq.queues
    WHERE name = $1
)
SELECT count(*) AS messages
FROM pgmq.messages
WHERE queue_id = (SELECT id FROM queue_info);