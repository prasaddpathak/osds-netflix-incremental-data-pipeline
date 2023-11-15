CREATE SCHEMA IF NOT EXISTS hdfs.osds;

DROP TABLE IF EXISTS hdfs.osds.source;

CREATE TABLE hdfs.osds.source
(
    clickid      BIGINT,
    datetime     TIMESTAMP,
    duration DOUBLE,
    title        VARCHAR,
    genres       VARCHAR,
    release_date DATE,
    movie_id     VARCHAR,
    user_id      VARCHAR,
    event_date   DATE
) with (
      format = 'PARQUET',
      partitioned_by = ARRAY['event_date']
      );