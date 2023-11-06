
def process_movie_analytics(*args):
    """Process clicks data to generate movie analytics"""

    import pandas as pd

    from sqlalchemy import create_engine

    SOURCE_TABLE = 'clicks'
    TARGET_TABLE = 'movie_analytics'
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
                            movie_id, title, genres,
                            ROUND(SUM(CASE WHEN duration > 0 THEN duration ELSE NULL END)/60, 2) as total_watch_time_min,
                            ROUND(AVG(CASE WHEN duration > 0 THEN duration ELSE NULL END)/60, 2) as avg_watch_time_min,
                            COUNT(clickid) as clicks,
                            SUM(CASE WHEN duration > 0 THEN 1 ELSE 0 END) as views,
                            SUM(CASE WHEN duration = 0 THEN 1 ELSE 0 END) as bounces,
                            SUM(CASE WHEN duration > 0 AND EXTRACT(HOUR FROM datetime) BETWEEN 6 AND 13 THEN 1 ELSE 0 END ) as morning_hour_viewers,
                            SUM(CASE WHEN duration > 0 AND EXTRACT(HOUR FROM datetime) BETWEEN 14 AND 21 THEN 1 ELSE 0 END ) as evening_hour_viewers,
                            SUM(CASE WHEN duration > 0 AND (EXTRACT(HOUR FROM datetime) >= 22 OR EXTRACT(HOUR FROM datetime)  <=  5) THEN 1 ELSE 0 END ) as night_hour_viewers,
                            event_date
                        FROM hdfs.{SCHEMA_NAME}.{SOURCE_TABLE}
                        WHERE 1=1
                            AND event_date= DATE '{event_date}'
                            AND same_day_next_click
                        GROUP BY event_date, movie_id, title, genres
                    """

    df = pd.read_sql(sql=INSERT_QUERY,
                     con=conn)

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
#     process_movie_analytics('2017-01-04')

