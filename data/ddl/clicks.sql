-- DROP TABLE hdfs.osds.clicks;

CREATE TABLE hdfs.osds.clicks
(
    same_day_next_click BOOLEAN,
    clickid             INT,
    datetime            TIMESTAMP,
    duration            DOUBLE,
    title               VARCHAR,
    genres              VARCHAR,
    release_date        DATE,
    movie_id            VARCHAR,
    user_id             VARCHAR,
    event_date          DATE
) with (
      format = 'PARQUET',
      partitioned_by = ARRAY['event_date']
      );