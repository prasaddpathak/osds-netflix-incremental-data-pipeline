DROP TABLE IF EXISTS hdfs.osds.user_analytics;

CREATE TABLE hdfs.osds.user_analytics
(
    user_id                 VARCHAR,
    total_watch_time_min    DOUBLE,
    avg_watch_time_min      DOUBLE,
    clicks                  INT,
    views                   INT,
    bounces                 INT,
    morning_hour_views      INT,
    evening_hour_views      INT,
    night_hour_views        INT,
    event_date              DATE
) with (
      format = 'PARQUET',
      partitioned_by = ARRAY['event_date']
      );