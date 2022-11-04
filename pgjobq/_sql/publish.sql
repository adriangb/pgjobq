WITH queue_info AS (
    SELECT
        name,
        id AS queue_id,
        max_delivery_attempts,
        retention_period
    FROM pgjobq.queues
    WHERE name = $1
), dependencies AS (
    INSERT INTO pgjobq.predecessors(queue_id, child_id, parent_id)
    SELECT
        (SELECT queue_id FROM queue_info),
        unnest($6::uuid[]),
        unnest($7::uuid[])
), new_messages AS (
    INSERT INTO pgjobq.jobs(
        queue_id,
        id,
        expires_at,
        delivery_attempts,
        available_at,
        body,
        attributes
    )
    SELECT
        queue_id,
        unnest($3::uuid[]),
        COALESCE($2, now()::timestamp) + retention_period,
        0,
        COALESCE($2, now()::timestamp),
        unnest($4::bytea[]),
        unnest($5::jsonb[])
    FROM queue_info
    RETURNING
        id
)
SELECT
    pg_notify('pgjobq.new_job_' || (SELECT name FROM queue_info), (SELECT count(*) FROM new_messages)::text)
;