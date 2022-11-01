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
