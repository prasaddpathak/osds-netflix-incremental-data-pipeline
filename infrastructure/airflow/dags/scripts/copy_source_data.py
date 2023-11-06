
def copy_source_data(*args):
    """Copy date from source for the given event date"""

    import pandas as pd

    from sqlalchemy import create_engine

    SOURCE_TABLE = 'source'
    TARGET_TABLE = 'clicks'
    SCHEMA_NAME = 'osds'

    event_date = args[0]
    print(f"Running for {event_date}")

    # conn = create_engine('trino://admin:@host.docker.internal:8085/hdfs')
    conn = create_engine('trino://admin:@localhost:8085/hdfs')

    print("Dropping existing partition")

    DELETE_QUERY = f"DELETE FROM hdfs.{SCHEMA_NAME}.{TARGET_TABLE} WHERE event_date= DATE '{event_date}'"

    with conn.connect() as con:
        con.execute(DELETE_QUERY)


    print("Creating new partition")

    INSERT_QUERY = f"""SELECT 
                                CASE WHEN DATE_ADD('second', CAST (duration as INT), date_parse(datetime, '%Y-%m-%d %H:%i:%S'))
                                    <= (CAST(event_date AS TIMESTAMP) + INTERVAL '28' HOUR)
                                        THEN TRUE ELSE FALSE END as same_day_next_click,
                                *
                        FROM hdfs.{SCHEMA_NAME}.{SOURCE_TABLE} 
                            WHERE 1=1 
                                AND event_date=DATE '{event_date}'
                    """

    df = pd.read_sql(sql=INSERT_QUERY,
                     con=conn,
                     parse_dates={
                         'datetime': '%Y-%m-%d %H:%M:%S',
                         'release_date': '%Y-%m-%d',
                     })

    print(f"Copying {len(df):} records")

    df.to_sql(name=TARGET_TABLE,
              schema=SCHEMA_NAME,
              con=conn,
              index=False,
              if_exists='append',
              method='multi')

    print('Partition write complete')

# Local Testing
if __name__ == "__main__":
    copy_source_data('2017-01-04')

