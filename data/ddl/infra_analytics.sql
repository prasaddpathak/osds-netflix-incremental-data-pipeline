DROP TABLE IF EXISTS hdfs.osds.infraanalytics;

CREATE TABLE hdfs.osds.infra_analytics
(
    event_hour              TIMESTAMP,
    total_watch_time_min    DOUBLE,
    clicks                  INT,
    views                   INT,
    bounces                 INT,
    event_date              DATE
) with (
      format = 'PARQUET',
      partitioned_by = ARRAY['event_date']
      );