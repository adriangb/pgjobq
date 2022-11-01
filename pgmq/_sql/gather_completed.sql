WITH input AS (
    SELECT
        id
    FROM unnest($2::uuid[]) AS t(id)
), queue_info AS (
    SELECT
        id
    FROM pgmq.queues
    WHERE name = $1
)
SELECT
    id
FROM input
WHERE NOT EXISTS(
    SELECT *
    FROM pgmq.messages
    WHERE (
        pgmq.messages.id = input.id
        AND
        pgmq.messages.queue_id = (SELECT id FROM queue_info)
    )
);