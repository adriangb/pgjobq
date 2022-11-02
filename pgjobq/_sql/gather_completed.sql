WITH input AS (
    SELECT
        id
    FROM unnest($2::uuid[]) AS t(id)
), queue_info AS (
    SELECT
        id
    FROM pgjobq.queues
    WHERE name = $1
)
SELECT
    id
FROM input
WHERE NOT EXISTS(
    SELECT *
    FROM pgjobq.jobs
    WHERE (
        pgjobq.jobs.id = input.id
        AND
        pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
    )
);