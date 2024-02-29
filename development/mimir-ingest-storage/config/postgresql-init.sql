-- This initialization file is executed only the first time, when the pgdata directory is empty.
-- When you change this file, you should delete the content of development/mimir-ingest-storage/.data-postgresql
-- to reinitialise the database.

CREATE TABLE segments (
    partition_id    smallint NOT NULL,
    offset_id       bigint NOT NULL,
    object_id       text NOT NULL
);

ALTER TABLE ONLY segments ADD CONSTRAINT "segments_primary_key" PRIMARY KEY (partition_id, offset_id);


CREATE TABLE consumer_offsets (
    partition_id    smallint NOT NULL,
    consumer_id     text NOT NULL,
    offset_id       bigint NOT NULL
);

ALTER TABLE ONLY consumer_offsets ADD CONSTRAINT "consumer_offsets_primary_key" PRIMARY KEY (partition_id, consumer_id);
