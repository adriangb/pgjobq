CREATE OR REPLACE FUNCTION pgmq.create_message_partitions() RETURNS trigger AS
$$
DECLARE
    messages_partition_table_name text;
BEGIN
    messages_partition_table_name :=  'messages_' || NEW.id::text;
    EXECUTE format(
        'CREATE TABLE IF NOT EXISTS pgmq.%I PARTITION OF pgmq.messages FOR VALUES IN (%L);',
        messages_partition_table_name,
        NEW.id
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "pgmq.create_message_partitions"
AFTER INSERT ON pgmq.queues FOR EACH ROW
EXECUTE PROCEDURE pgmq.create_message_partitions();

CREATE OR REPLACE FUNCTION pgmq.drop_messages_partitions() RETURNS trigger AS
$$
DECLARE
    messages_partition_table_name text;
BEGIN
    messages_partition_table_name :=  'messages_' || OLD.id::text;
    EXECUTE format(
        'DROP TABLE IF EXISTS pgmq.%I CASCADE;',
        messages_partition_table_name
    );
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "pgmq.drop_messages_partitions"
AFTER DELETE ON pgmq.queues FOR EACH ROW
EXECUTE PROCEDURE pgmq.drop_messages_partitions();

CREATE OR REPLACE FUNCTION pgmq.on_message_create() RETURNS trigger AS
$$
DECLARE
    channel_name text;
    queue_name text;
BEGIN
    SELECT name
    INTO queue_name
    FROM pgmq.queues
    WHERE (
        pgmq.queues.id = NEW.queue_id
    );
    channel_name := 'pgmq.new_message_' || queue_name;
    PERFORM pg_notify(channel_name, '');
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "pgmq.on_message_create"
BEFORE UPDATE OR INSERT ON pgmq.messages
FOR EACH ROW
EXECUTE FUNCTION pgmq.on_message_create();


CREATE OR REPLACE FUNCTION pgmq.on_message_delete() RETURNS trigger AS
$$
DECLARE
    channel_name text;
    queue_name text;
BEGIN
    SELECT name
    INTO queue_name
    FROM pgmq.queues
    WHERE (
        pgmq.queues.id = OLD.queue_id
    );
    channel_name := 'pgmq.message_completed_' || queue_name;
    PERFORM pg_notify(channel_name, OLD.id::text);
    RETURN OLD;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER "pgmq.on_message_delete"
BEFORE DELETE ON pgmq.messages
FOR EACH ROW
EXECUTE FUNCTION pgmq.on_message_delete();
