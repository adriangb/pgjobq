WITH queue_info AS (
    SELECT
        id,
        name,
        max_delivery_attempts,
        retention_period
    FROM pgjobq.queues
    WHERE name = $1
), selected_jobs AS (
    SELECT
        id
    FROM pgjobq.jobs
    WHERE (
        queue_id = (SELECT id FROM queue_info)
        {where}
    )
    ORDER BY id
    FOR UPDATE
), deleted_jobs AS (
    DELETE FROM pgjobq.jobs
    USING selected_jobs
    WHERE (
        pgjobq.jobs.queue_id = (SELECT id FROM queue_info)
        AND
        pgjobq.jobs.id = selected_jobs.id
    )
), partitioned_completed_job_ids AS (
    -- NOTIFY has a max payload length of 8000 chars
    -- each id is a uuid of 36 characters, plus a comma, so 37
    -- thus we can fit ~200 ids in each notification
    SELECT
        id,
        row_number() OVER (ORDER BY 1) / 200 AS batch_group
    FROM selected_jobs
), completed_notification_groups AS (
    SELECT
        string_agg(id::text, ',') AS ids,
        (SELECT name FROM queue_info) AS queue_name
    FROM partitioned_completed_job_ids
    GROUP BY batch_group
), deleted_notify AS (
    SELECT pg_notify('pgjobq.job_completed_' || queue_name, ids) AS notification
    FROM completed_notification_groups
)
SELECT
    1
FROM deleted_notify
GROUP BY 1
;