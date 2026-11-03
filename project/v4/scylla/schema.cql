-- Per-event rows written by the Redpanda Connect consumer.
-- The primary key is derived from the event itself (minute, partition, offset),
-- so replaying a Kafka offset overwrites the same row instead of duplicating it.
CREATE TABLE IF NOT EXISTS recent_change_events (
    minute_bucket text,
    partition int,
    offset bigint,
    event_id bigint,
    event_timestamp bigint,
    type text,
    namespace int,
    title text,
    username text,
    bot boolean,
    minor boolean,
    server_url text,
    server_name text,
    wiki text,
    comment text,
    PRIMARY KEY ((minute_bucket), partition, offset)
) WITH CLUSTERING ORDER BY (partition ASC, offset ASC);
