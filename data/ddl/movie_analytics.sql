DROP TABLE IF EXISTS hdfs.osds.movie_analytics;

CREATE TABLE hdfs.osds.movie_analytics
(
    movie_id                VARCHAR,
    title                   VARCHAR,
    genres                  VARCHAR,
    total_watch_time_min    DOUBLE,
    avg_watch_time_min      DOUBLE,
    clicks                  INT,
    views                   INT,
    bounces                 INT,
    morning_hour_viewers    INT,
    evening_hour_viewers    INT,
    night_hour_viewers      INT,
    event_date              DATE
) with (
      format = 'PARQUET',
      partitioned_by = ARRAY['event_date']
      );