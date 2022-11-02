CREATE SCHEMA pgjobq;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

create table pgjobq.migrations (
    current_revision smallint not null
);

INSERT INTO pgjobq.migrations VALUES (0);

create table pgjobq.queues (
    id serial primary key,
    name text not null,
    UNIQUE(name),
    ack_deadline interval not null,
    max_delivery_attempts integer not null,
    retention_period interval not null,
    max_size bigint
);

create table pgjobq.jobs (
    queue_id serial references pgjobq.queues on delete cascade not null,
    id uuid, -- generated by app so it can start waiting for results before publishing
    PRIMARY KEY(queue_id, id),
    expires_at timestamp not null,
    delivery_attempts_remaining integer not null,
    available_at timestamp not null,
    body bytea not null,
    attributes jsonb
) PARTITION BY LIST(queue_id);

create table pgjobq.predecessors (
    queue_id serial references pgjobq.queues on delete cascade not null,
    child_id uuid not null,
    parent_id uuid not null,
    PRIMARY KEY(queue_id, parent_id, child_id),
    FOREIGN KEY (queue_id, child_id) REFERENCES pgjobq.jobs (queue_id, id) ON DELETE CASCADE,
    FOREIGN KEY (queue_id, parent_id) REFERENCES pgjobq.jobs (queue_id, id) ON DELETE CASCADE
);

-- For looking up "all of this job's parents" when we ack or cancel a job
CREATE INDEX "pgjobq.predecessors_child_id_idx" ON pgjobq.predecessors(child_id);

create table pgjobq.queue_link_types(
    id serial primary key,
    name text not null,
    UNIQUE(name)
);

-- TODO: fan-out, reply-to?
INSERT INTO pgjobq.queue_link_types(name)
VALUES ('dlq');

create table pgjobq.queue_links(
    id serial primary key,
    parent_id serial references pgjobq.queues on delete cascade not null,
    link_type_id serial references pgjobq.queue_link_types on delete cascade not null,
    child_id serial references pgjobq.queues on delete cascade not null,
    UNIQUE(parent_id, link_type_id, child_id)
);

-- Index for ackinc/nacknig jobs within a partition
CREATE INDEX "pgjobq.jobs_id_idx" ON pgjobq.jobs(id);

-- Indexes for looking for expired jobs and available jobs
CREATE INDEX "pgjobq.jobs_available_idx"
ON pgjobq.jobs(available_at);

CREATE INDEX "pgjobq.jobs_expiration_idx"
ON pgjobq.jobs(delivery_attempts_remaining, expires_at);

-- Triggers for managing partitions
CREATE OR REPLACE FUNCTION pgjobq.create_job_partitions() RETURNS trigger AS
$$
DECLARE
    jobs_partition_table_name text;
BEGIN
    jobs_partition_table_name :=  'jobs_' || NEW.id::text;
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS pgjobq.%I PARTITION OF pgjobq.jobs FOR VALUES IN (%L);',
        jobs_partition_table_name,
        NEW.id
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "pgjobq.create_job_partitions"
AFTER INSERT ON pgjobq.queues FOR EACH ROW
EXECUTE PROCEDURE pgjobq.create_job_partitions();

CREATE OR REPLACE FUNCTION pgjobq.drop_jobs_partitions() RETURNS trigger AS
$$
DECLARE
    jobs_partition_table_name text;
BEGIN
    jobs_partition_table_name :=  'jobs_' || OLD.id::text;
    EXECUTE format(
        'DROP TABLE IF EXISTS pgjobq.%I CASCADE;',
        jobs_partition_table_name
    );
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;