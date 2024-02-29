-- This initialization file is executed only the first time, when the pgdata directory is empty.
-- When you change this file, you should delete the content of development/mimir-ingest-storage/.data-postgresql
-- to reinitialise the database.

CREATE TABLE segments (
    partition_id    smallint NOT NULL,
    offset_id       bigint NOT NULL,
    object_id       text NOT NULL
);

ALTER TABLE ONLY segments ADD CONSTRAINT "primary_key" PRIMARY KEY (partition_id, commit_id);
