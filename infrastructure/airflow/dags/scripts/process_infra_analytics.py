
def process_infra_analytics(*args):
    """Process clicks data to generate infra analytics"""

    import pandas as pd
    from sqlalchemy import create_engine

    SOURCE_TABLE = 'clicks'
    TARGET_TABLE = 'infra_analytics'
    SCHEMA_NAME = 'osds'

    event_date = args[0]
    print(f"Running for {event_date}")

    conn = create_engine('trino://admin:@host.docker.internal:8085/hdfs')
    # conn = create_engine('trino://admin:@localhost:8085/hdfs')    # For Pythonic Run

    print(f"Dropping existing partition for {SCHEMA_NAME}.{TARGET_TABLE}")

    DELETE_QUERY = f"DELETE FROM hdfs.{SCHEMA_NAME}.{TARGET_TABLE} WHERE event_date= DATE '{event_date}'"

    with conn.connect() as con:
        con.execute(DELETE_QUERY)

    print(f"Creating new partition for {SCHEMA_NAME}.{TARGET_TABLE}")

    INSERT_QUERY = f"""SELECT
                            CAST(date_format(datetime, '%Y-%m-%d %H:00:00') AS TIMESTAMP) as event_hour,
                            ROUND(SUM(CASE WHEN duration > 0 THEN duration ELSE NULL END)/60, 2) as total_watch_time_min,
                            COUNT(clickid) as clicks,
                            SUM(CASE WHEN duration > 0 THEN 1 ELSE 0 END) as views,
                            SUM(CASE WHEN duration = 0 THEN 1 ELSE 0 END) as bounces,
                            event_date
                        FROM hdfs.{SCHEMA_NAME}.{SOURCE_TABLE}
                        WHERE 1=1
                            AND event_date= DATE '{event_date}'
                            AND same_day_next_click
                        GROUP BY event_date,  date_format(datetime, '%Y-%m-%d %H:00:00')
                    """

    df = pd.read_sql(sql=INSERT_QUERY,
                     con=conn,
                     parse_dates={
                         'event_hour': '%Y-%m-%d %H:%M:%S',
                     })

    print(f"Writing {len(df):} records to {SCHEMA_NAME}.{TARGET_TABLE}")

    df.to_sql(name=TARGET_TABLE,
              schema=SCHEMA_NAME,
              con=conn,
              index=False,
              if_exists='append',
              method='multi')

    print('Partition write complete')


# Local Testing
# if __name__ == "__main__":
#     process_infra_analytics('2017-01-04')

